package qldriver

import (
	"bytes"
	"errors"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"net"
	"time"
)

func splitMessage(message []byte) (uuid.UUID, error) {
	before, after, found := bytes.Cut(message, []byte(";"))

	if !found {
		return [16]byte{}, errors.New("message formatted incorrectly")
	}

	if !bytes.Equal(before, []byte("greetings")) {
		return [16]byte{}, errors.New("message formatted incorrectly")
	}

	return uuid.FromBytes(after)
}

func getEncodedChallenge(username, password string, isPub bool, uid uuid.UUID) ([]byte, error) {

	pAndC := make([]byte, len(password)+16)

	copy(pAndC, password)
	copy(pAndC[len(password):], uid[:])

	pass, err := bcrypt.GenerateFromPassword(pAndC, bcrypt.DefaultCost)

	if err != nil {
		return nil, err
	}

	response := make([]byte, 2+len(username)+len(pass))

	role := uint8(1)
	if isPub {
		role = 0
	}

	response[0] = role
	copy(response[1:], username)
	response[len(username)+1] = ';'
	copy(response[len(username)+2:], pass)

	return response, nil
}

func authenticate(conn net.Conn, deadline time.Duration, username, password string, isPub bool) (bool, error) {
	challenge, alive, err := readMessage(conn, deadline)

	if !alive {
		return alive, err
	}

	uid, err := splitMessage(challenge)

	if err != nil {
		closeConn(conn)
		return false, err
	}

	creds, err := getEncodedChallenge(username, password, isPub, uid)

	if err != nil {
		closeConn(conn)
	}

	alive, err = writeMessage(conn, deadline, creds)

	if !alive {
		return alive, err
	}

	mess, alive, err := readMessage(conn, deadline)

	if !alive {
		return alive, err
	}

	mess, err = splitResponse(mess)

	if err != nil {
		closeConn(conn)
		return false, err
	}

	return true, nil
}
