package qldriver

import (
	"encoding/binary"
	"github.com/google/uuid"
	"net"
	"time"
)

func createRequest(typ, data []byte) []byte {
	mess := newMessage(uint32(len(typ) + len(data) + 1))
	copy(mess, typ)
	mess[len(typ)] = ';'
	copy(mess[len(typ)+1:], data)
	return mess
}

func push(conn net.Conn, deadline time.Duration, message []byte) (pos uint32, alive bool, err error) {

	alive, err = writeMessage(conn, deadline, createRequest([]byte("PUSH"), message))

	if !alive {
		return 0, alive, err
	}

	message, alive, err = readMessage(conn, deadline)

	if !alive {
		return 0, alive, err
	}

	data, err := splitResponse(message)

	if err != nil {
		return 0, true, err
	}

	deleteMessage(message)

	return binary.LittleEndian.Uint32(data), true, nil
}

func length(conn net.Conn, deadline time.Duration) (length uint32, alive bool, err error) {

	alive, err = writeMessage(conn, deadline, createRequest([]byte("LEN"), []byte{}))

	if !alive {
		return 0, alive, err
	}

	message, alive, err := readMessage(conn, deadline)

	if !alive {
		return 0, alive, err
	}

	data, err := splitResponse(message)
	deleteMessage(message)

	if err != nil {
		return 0, true, err
	}

	return binary.LittleEndian.Uint32(data), true, nil
}

func splitPopResponse(resp []byte) (uid uuid.UUID, mess []byte, err error) {
	data, err := splitResponse(resp)

	if err != nil {
		return [16]byte{}, nil, err
	}

	uid, err = uuid.FromBytes(data[:16])

	data = data[17:]

	if err != nil {
		return [16]byte{}, nil, err
	}

	return uid, data, nil
}

func hideAndPoll(conn net.Conn, deadline time.Duration, hiddenSeconds uint32, isPoll bool) (
	uid uuid.UUID,
	message []byte,
	alive bool,
	err error) {

	typ := "HIDE"

	if isPoll {
		typ = "POLL"
	}

	hiddenBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(hiddenBuffer, hiddenSeconds)

	alive, err = writeMessage(conn, deadline, createRequest([]byte(typ), hiddenBuffer))

	if !alive {
		return [16]byte{}, nil, alive, err
	}

	message, alive, err = readMessage(conn, deadline)

	if !alive {
		return [16]byte{}, nil, alive, err
	}

	uid, data, err := splitPopResponse(message)

	if err != nil {
		return [16]byte{}, nil, true, err
	}

	return uid, data, alive, nil
}

func del(conn net.Conn, deadline time.Duration, delUid uuid.UUID) (uid uuid.UUID, alive bool, err error) {

	alive, err = writeMessage(conn, deadline, createRequest([]byte("DEL"), delUid[:]))

	if !alive {
		return [16]byte{}, alive, err
	}

	message, alive, err := readMessage(conn, deadline)

	if !alive {
		return [16]byte{}, alive, err
	}

	mess, err := splitResponse(message)

	if err != nil {
		return [16]byte{}, true, err
	}

	uid, err = uuid.FromBytes(mess)
	deleteMessage(message)

	if err != nil {
		return [16]byte{}, true, err
	}

	return
}
