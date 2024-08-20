package qldriver

import (
	"bytes"
	"encoding/binary"
	"github.com/google/uuid"
	"testing"
	"time"
)

func Test_createRequest(t *testing.T) {
	typ := "test"
	data := ";;;;oooga;;boooga"
	fullString := "test;;;;;oooga;;boooga"

	resp := createRequest([]byte(typ), []byte(data))

	if !bytes.Equal(resp, []byte(fullString)) {
		t.Fatal("expected result not acheived")
	}
}

func Test_push(t *testing.T) {
	type pushResp struct {
		pos   uint32
		alive bool
		err   error
	}

	t.Run("test proper push", func(t *testing.T) {
		respChan := make(chan pushResp)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			pos, alive, err := push(clientConn, time.Second*4, []byte("test message"))
			respChan <- pushResp{
				pos, alive, err,
			}
		}()

		ps := make([]byte, 4)
		binary.LittleEndian.PutUint32(ps, 100)

		m := []byte("PASS;")
		m = append(m, ps...)

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("PUSH")) {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[1], []byte("test message")) {
			t.Fatal("improper message received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.pos != 100 {
			t.Fatal("incorrect pos")
		}
	})

	t.Run("test failed push", func(t *testing.T) {
		respChan := make(chan pushResp)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			pos, alive, err := push(clientConn, time.Second*4, []byte("test message"))
			respChan <- pushResp{
				pos, alive, err,
			}
		}()

		ps := []byte("FAIL;failed for test")

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("PUSH")) {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[1], []byte("test message")) {
			t.Fatal("improper message received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, ps)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.err == nil {
			t.Fatal("incorrect err")
		}
	})
}

func Test_length(t *testing.T) {
	type pushResp struct {
		pos   uint32
		alive bool
		err   error
	}

	t.Run("test proper length", func(t *testing.T) {
		respChan := make(chan pushResp)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			pos, alive, err := length(clientConn, time.Second*4)
			respChan <- pushResp{
				pos, alive, err,
			}
		}()

		ps := make([]byte, 4)
		binary.LittleEndian.PutUint32(ps, 100)

		m := []byte("PASS;")
		m = append(m, ps...)

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("LEN")) {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[1], []byte{}) {
			t.Fatal("improper message received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.pos != 100 {
			t.Fatal("incorrect pos")
		}
	})

	t.Run("test failed length", func(t *testing.T) {
		respChan := make(chan pushResp)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			pos, alive, err := length(clientConn, time.Second*4)
			respChan <- pushResp{
				pos, alive, err,
			}
		}()

		ps := []byte("FAIL;failed for test")

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("LEN")) {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[1], []byte{}) {
			t.Fatal("improper message received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, ps)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.err == nil {
			t.Fatal("incorrect err")
		}
	})
}

func Test_splitPopResponse(t *testing.T) {

	t.Run("test splitPop pass", func(t *testing.T) {
		mess := []byte("PASS;")
		uid, err := uuid.NewUUID()

		if err != nil {
			t.Error(err)
		}

		mess = append(mess, uid[:]...)
		mess = append(mess, []byte(";")...)
		mess = append(mess, []byte("test message")...)

		guid, mess, err := splitPopResponse(mess)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal(uid[:], guid[:]) {
			t.Fatal("uid do not match")
		}

		if !bytes.Equal(mess, []byte("test message")) {
			t.Fatal("messages do not match")
		}
	})

	t.Run("test splitPop fail", func(t *testing.T) {
		mess := []byte("FAIL;")
		uid, err := uuid.NewUUID()

		if err != nil {
			t.Error(err)
		}

		mess = append(mess, uid[:]...)
		mess = append(mess, []byte(";")...)
		mess = append(mess, []byte("test failure")...)

		_, mess, err = splitPopResponse(mess)

		if err == nil {
			t.Fatal("did not fail")
		}
	})
}

func Test_hideAndPoll(t *testing.T) {

	type hideRep struct {
		uid     uuid.UUID
		message []byte
		alive   bool
		err     error
	}

	t.Run("test hide pass", func(t *testing.T) {
		respChan := make(chan hideRep)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			uid, message, alive, err := hideAndPoll(clientConn, time.Second*4, 10, true)
			respChan <- hideRep{
				uid, message, alive, err,
			}
		}()

		ps := []byte("test message")

		m := []byte("PASS;")
		uid, err := uuid.NewUUID()

		if err != nil {
			t.Error(err)
		}

		m = append(m, uid[:]...)
		m = append(m, []byte(";")...)
		m = append(m, ps...)

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("POLL")) {
			t.Fatal("improper message received")
		}

		hTime := binary.LittleEndian.Uint32(parts[1])

		if hTime != 10 {
			t.Fatal("improper hidden time received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if !bytes.Equal(resp.message, ps) {
			t.Fatal("message is not the same")
		}

		if !bytes.Equal(resp.uid[:], uid[:]) {
			t.Fatal("message is not the same")
		}
	})

	t.Run("test poll pass", func(t *testing.T) {
		respChan := make(chan hideRep)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			uid, message, alive, err := hideAndPoll(clientConn, time.Second*4, 10, false)
			respChan <- hideRep{
				uid, message, alive, err,
			}
		}()

		ps := []byte("test message")

		m := []byte("PASS;")
		uid, err := uuid.NewUUID()

		if err != nil {
			t.Error(err)
		}

		m = append(m, uid[:]...)
		m = append(m, []byte(";")...)
		m = append(m, ps...)

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("HIDE")) {
			t.Fatal("improper message received")
		}

		hTime := binary.LittleEndian.Uint32(parts[1])

		if hTime != 10 {
			t.Fatal("improper hidden time received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if !bytes.Equal(resp.message, ps) {
			t.Fatal("message is not the same")
		}

		if !bytes.Equal(resp.uid[:], uid[:]) {
			t.Fatal("message is not the same")
		}
	})

	t.Run("test hide&Poll fail", func(t *testing.T) {
		respChan := make(chan hideRep)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		go func() {
			uid, message, alive, err := hideAndPoll(clientConn, time.Second*4, 10, false)
			respChan <- hideRep{
				uid, message, alive, err,
			}
		}()

		ps := []byte("test failed message")
		m := []byte("FAIL;")
		m = append(m, ps...)

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("HIDE")) {
			t.Fatal("improper message received")
		}

		hTime := binary.LittleEndian.Uint32(parts[1])

		if hTime != 10 {
			t.Fatal("improper hidden time received")
		}

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.err == nil {
			t.Fatal("response should have failed")
		}
	})
}

func Test_del(t *testing.T) {
	type delRep struct {
		uid   uuid.UUID
		alive bool
		err   error
	}

	t.Run("test del", func(t *testing.T) {
		respChan := make(chan delRep)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		uid, err := uuid.NewUUID()
		if err != nil {
			t.Error(err)
		}

		go func() {
			uid, alive, err := del(clientConn, time.Second*4, uid)
			respChan <- delRep{
				uid, alive, err,
			}
		}()

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("DEL")) {
			t.Fatal("improper message received")
		}

		messUid, err := uuid.FromBytes(parts[1])

		if !bytes.Equal(messUid[:], uid[:]) {
			t.Fatal("uid does not match")
		}

		m := []byte("PASS;")
		m = append(m, uid[:]...)

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if !bytes.Equal(resp.uid[:], uid[:]) {
			t.Fatal("uid does not match")
		}
	})

	t.Run("test del fail", func(t *testing.T) {
		respChan := make(chan delRep)

		ts := newTestServer()
		serverConn, clientConn, err := ts.getServerConn()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
			ts.endServer(t)
		}()

		uid, err := uuid.NewUUID()
		if err != nil {
			t.Error(err)
		}

		go func() {
			uid, alive, err := del(clientConn, time.Second*4, uid)
			respChan <- delRep{
				uid, alive, err,
			}
		}()

		mess, alive, err := readMessage(serverConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		parts := bytes.Split(mess, []byte(";"))

		if len(parts) != 2 {
			t.Fatal("improper message received")
		}

		if !bytes.Equal(parts[0], []byte("DEL")) {
			t.Fatal("improper message received")
		}

		messUid, err := uuid.FromBytes(parts[1])

		if !bytes.Equal(messUid[:], uid[:]) {
			t.Fatal("uid does not match")
		}

		m := []byte("FAIL;")
		m = append(m, []byte("testing failure")...)

		alive, err = writeMessage(serverConn, time.Second*4, m)

		if !alive {
			t.Fatal(err)
		}

		resp := <-respChan

		if !resp.alive {
			t.Fatal(resp.err)
		}

		if resp.err == nil {
			t.Fatal("failed to catch error")
		}
	})
}
