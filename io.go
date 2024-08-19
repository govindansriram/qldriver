package qldriver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"
)

var lengthPool = sync.Pool{
	New: func() any {
		return [4]byte{}
	},
}

func newLength() [4]byte {
	val := lengthPool.Get()
	if l, ok := val.([4]byte); ok {
		return l
	} else {
		panic("could not cast to length")
	}
}

func deleteLength(length [4]byte) {
	lengthPool.Put(length)
}

var messagePool = sync.Pool{
	New: func() any {
		return struct{}{}
	},
}

func newMessage(defaultSize uint32) []byte {
	val := messagePool.Get()
	if l, ok := val.([]byte); ok {
		if uint32(len(l)) > defaultSize {
			l = l[:defaultSize]
		}

		if uint32(len(l)) < defaultSize {
			difference := make([]byte, defaultSize-uint32(len(l)))
			l = append(l, difference...)
		}

		return l
	} else {
		return make([]byte, defaultSize)
	}
}

func deleteMessage(mess []byte) {
	messagePool.Put(mess)
}

/*
closeConn

close the connection and log the error message
*/
func closeConn(conn net.Conn) {
	_ = conn.Close()
}

func readConnection(conn net.Conn, buffer []byte) (alive bool, err error) {
	total := 0
	for total < 4 { // read the remaining size bytes if needed
		n, err := conn.Read(buffer)

		if err != nil {
			closeConn(conn)
			return false, err
		}

		total += n
		buffer = buffer[n:]
	}

	return true, nil
}

/*
readMessageLength

extracts the length of server message, this length is then used to determine when the message is done being
read
*/
func readMessageLength(conn net.Conn) (length uint32, alive bool, err error) {
	fullBuffer := newLength()
	alive, err = readConnection(conn, fullBuffer[:])

	if !alive {
		return 0, false, err
	}

	length = binary.LittleEndian.Uint32(fullBuffer[:])
	deleteLength(fullBuffer)

	return length, true, nil
}

/*
readMessage

reads a client side message
*/
func readMessage(conn net.Conn, deadline time.Duration) (message []byte, alive bool, err error) {

	err = conn.SetReadDeadline(time.Now().Add(deadline)) // entire message must be read in this time
	if err != nil {
		closeConn(conn)
		return
	}

	length, status, err := readMessageLength(conn)

	if !status {
		return nil, false, err
	}

	buffer := newMessage(length)

	alive, err = readConnection(conn, buffer)

	if !alive {
		return nil, alive, err
	}

	err = conn.SetReadDeadline(time.Time{}) // remove deadline for future connections

	if err != nil {
		closeConn(conn)
		return
	}

	return buffer, true, nil
}

func writeToConnection(conn net.Conn, message []byte) (bool, error) {
	total := 0

	for total < len(message) {
		n, err := conn.Write(message)

		if err != nil {
			closeConn(conn)
			return false, net.ErrClosed
		}

		total += n
		message = message[n:]
	}

	return true, nil
}

func writeMessage(conn net.Conn, deadline time.Duration, message []byte) (bool, error) {
	buffer := newLength()
	binary.LittleEndian.PutUint32(buffer[:], uint32(len(message)))

	err := conn.SetWriteDeadline(time.Now().Add(deadline)) // entire message must be read in this time

	if err != nil {
		deleteLength(buffer)
		return false, nil
	}

	alive, err := writeToConnection(conn, buffer[:])

	if !alive {
		return alive, err
	}

	alive, err = writeToConnection(conn, message)
	deleteLength(buffer)
	deleteMessage(message)

	if !alive {
		return alive, err
	}

	err = conn.SetWriteDeadline(time.Time{}) // entire message must be read in this time

	if err != nil {
		return false, nil
	}

	return true, nil
}

func splitResponse(message []byte) ([]byte, error) {

	before, after, found := bytes.Cut(message, []byte(";"))

	if !found {
		return nil, errors.New("response is improperly formatted")
	}

	if bytes.Equal(before, []byte("PASS")) {
		return after, nil
	}

	if bytes.Equal(before, []byte("FAIL")) {
		return nil, errors.New(string(after))
	}

	return nil, errors.New("response is improperly formatted")
}
