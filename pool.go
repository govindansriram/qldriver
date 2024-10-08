package qldriver

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"net"
	"time"
)

type connectionPool struct {
	username              string
	password              string
	maxIoTimeSeconds      uint16
	maxPollingTimeSeconds uint16
	address               string
	port                  uint16
	isPub                 bool
	utilizedConnections   chan struct{}
	readyConnections      chan net.Conn
	kill                  chan struct{}
}

func (s *connectionPool) createConnection() (net.Conn, error) {
	address := fmt.Sprintf("%s:%d", s.address, s.port)
	timeout := 1 * time.Second
	conn, err := net.DialTimeout("tcp", address, timeout)

	if err != nil {
		return nil, err
	}

	alive, err := authenticate(
		conn,
		time.Second*time.Duration(s.maxIoTimeSeconds),
		s.username,
		s.password,
		s.isPub)

	if !alive {
		return nil, err
	}

	return conn, nil
}

func (s *connectionPool) connectionWorker() {
	for {
		select {
		case <-s.kill:
			return
		case s.utilizedConnections <- struct{}{}:
			select {
			case <-s.kill:
				return
			default:
			}

			c, err := s.createConnection()

			if err != nil {
				time.Sleep(3 * time.Second)
				<-s.utilizedConnections
				continue
			}

			s.readyConnections <- c
		}
	}
}

func (s *connectionPool) getConnection() (net.Conn, error) {
	select {
	case conn := <-s.readyConnections:
		select {
		case <-s.kill:
			return nil, errors.New("session ended")
		default:
		}

		return conn, nil
	case <-s.kill:
		return nil, errors.New("session ended")
	}
}

func (s *connectionPool) len() (int, error) {
	conn, err := s.getConnection()
	if err != nil {
		return -1, err
	}

	lth, alive, err := length(conn, time.Duration(s.maxIoTimeSeconds)*time.Second)

	if !alive {
		closeConn(conn)
		<-s.utilizedConnections
		return -1, err
	}

	s.readyConnections <- conn

	if err != nil {
		return -1, err
	}

	return int(lth), nil
}

func (s *connectionPool) close() {
	close(s.kill)
}

type PublisherClient struct {
	cPool connectionPool
}

type SubscriberClient struct {
	cPool connectionPool
}

func NewPublisherClient(
	username,
	password string,
	maxConnections,
	maxIoTimeSeconds,
	port uint16,
	address string) (PublisherClient, error) {

	cp := connectionPool{
		username:            username,
		password:            password,
		maxIoTimeSeconds:    maxIoTimeSeconds,
		kill:                make(chan struct{}),
		utilizedConnections: make(chan struct{}, maxConnections),
		readyConnections:    make(chan net.Conn, maxConnections),
		isPub:               true,
		port:                port,
		address:             address,
	}

	sc := PublisherClient{
		cPool: cp,
	}

	go func() {
		sc.cPool.connectionWorker()
	}()

	filledStruct := make(chan struct{})

	go func() {
		for {
			if len(sc.cPool.readyConnections) > 0 {
				filledStruct <- struct{}{}
				return
			}
		}
	}()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*5)

	defer cf()

	select {
	case <-ctx.Done():
		sc.cPool.close()
		return PublisherClient{}, errors.New("could not connect to qlite server")
	case <-filledStruct:
		return sc, nil
	}
}

func (pc PublisherClient) Len() (int, error) {
	return pc.cPool.len()
}

func (pc PublisherClient) Close() {
	pc.cPool.close()
}

func (pc PublisherClient) Push(mess []byte) (int, error) {
	conn, err := pc.cPool.getConnection()
	if err != nil {
		return -1, err
	}

	pos, alive, err := push(conn, time.Second*time.Duration(pc.cPool.maxIoTimeSeconds), mess)
	deleteMessage(mess)

	if !alive {
		closeConn(conn)
		<-pc.cPool.utilizedConnections
		return -1, err
	}

	pc.cPool.readyConnections <- conn

	if err != nil {
		return -1, err
	}

	return int(pos), nil
}

func NewSubscriberClient(
	username,
	password string,
	maxConnections,
	maxIoTimeSeconds,
	maxPollingTimeSeconds,
	port uint16,
	address string) (SubscriberClient, error) {

	cp := connectionPool{
		username:              username,
		password:              password,
		maxIoTimeSeconds:      maxIoTimeSeconds,
		maxPollingTimeSeconds: maxPollingTimeSeconds,
		kill:                  make(chan struct{}),
		utilizedConnections:   make(chan struct{}, maxConnections),
		readyConnections:      make(chan net.Conn, maxConnections),
		isPub:                 false,
		port:                  port,
		address:               address,
	}

	sc := SubscriberClient{
		cPool: cp,
	}

	go func() {
		sc.cPool.connectionWorker()
	}()

	filledStruct := make(chan struct{})

	go func() {
		for {
			if len(sc.cPool.readyConnections) > 0 {
				filledStruct <- struct{}{}
				return
			}
		}
	}()

	ctx, cf := context.WithTimeout(context.Background(), time.Second*5)

	defer cf()

	select {
	case <-ctx.Done():
		sc.cPool.close()
		return SubscriberClient{}, errors.New("could not connect to qlite server")
	case <-filledStruct:
		return sc, nil
	}
}

func (sc SubscriberClient) Len() (int, error) {
	return sc.cPool.len()
}

func (sc SubscriberClient) Close() {
	sc.cPool.close()
}

func (sc SubscriberClient) hideOrPoll(hiddenTime uint16, isPoll bool) (uuid.UUID, []byte, error) {
	conn, err := sc.cPool.getConnection()

	totalTime := uint32(sc.cPool.maxIoTimeSeconds)
	if err != nil {
		return [16]byte{}, nil, err
	}

	if isPoll {
		totalTime += uint32(sc.cPool.maxPollingTimeSeconds)
	}

	uid, mess, alive, err := hideAndPoll(
		conn,
		time.Duration(totalTime)*time.Second,
		uint32(hiddenTime),
		isPoll)

	if !alive {
		closeConn(conn)
		<-sc.cPool.utilizedConnections
		return [16]byte{}, nil, err
	}

	sc.cPool.readyConnections <- conn

	if err != nil {
		return [16]byte{}, nil, err
	}

	return uid, mess, nil
}

func (sc SubscriberClient) Hide(hiddenTime uint16) (uuid.UUID, []byte, error) {
	return sc.hideOrPoll(hiddenTime, false)
}

func (sc SubscriberClient) Poll(hiddenTime uint16) (uuid.UUID, []byte, error) {
	return sc.hideOrPoll(hiddenTime, true)
}

func (sc SubscriberClient) Delete(uid uuid.UUID) (uuid.UUID, error) {
	conn, err := sc.cPool.getConnection()
	if err != nil {
		return [16]byte{}, err
	}

	uid, alive, err := del(
		conn,
		time.Duration(sc.cPool.maxPollingTimeSeconds)*time.Second,
		uid)

	if !alive {
		closeConn(conn)
		<-sc.cPool.utilizedConnections
		return [16]byte{}, err
	}

	sc.cPool.readyConnections <- conn

	if err != nil {
		return [16]byte{}, err
	}

	return uid, nil
}
