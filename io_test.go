package qldriver

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

type testServer struct {
	kill     chan struct{}
	connChan chan net.Conn
	errChan  chan error
}

func newTestServer() testServer {
	killChannel := make(chan struct{})
	connectionChannel := make(chan net.Conn)
	errorChannel := make(chan error)

	return testServer{
		kill:     killChannel,
		connChan: connectionChannel,
		errChan:  errorChannel,
	}
}

func (t testServer) startServer() {

	listener, err := net.Listen("tcp", "localhost:8080")

	if err != nil {
		t.errChan <- err
		return
	}

	connectionChannel := make(chan net.Conn)
	errorChannel := make(chan error)

	go func() {
		serverConn, err := listener.Accept()

		if err != nil {
			_ = listener.Close()
			errorChannel <- err
			return
		}

		connectionChannel <- serverConn
	}()

	var serverConn net.Conn

	select {
	case err = <-errorChannel:
		t.errChan <- err
		return
	case serverConn = <-connectionChannel:
		t.connChan <- serverConn
	}

	defer func() {
		_ = serverConn.Close()
		_ = listener.Close()
	}()

	for {
		select {
		case <-t.kill:
			_ = serverConn.Close()
			err = listener.Close()
			t.errChan <- err
			return
		default:
		}
	}
}

func (t testServer) getServerConn() (server net.Conn, client net.Conn, err error) {
	go t.startServer()

	clientChan := make(chan net.Conn)
	clientErrChan := make(chan error)
	for {
		go func() {
			clientConn, err := net.Dial("tcp", "localhost:8080")
			if err == nil {
				clientChan <- clientConn
			} else {
				clientErrChan <- err
			}
		}()

		select {
		case client = <-clientChan:
			server = <-t.connChan
			return server, client, err
		case err = <-t.errChan:
			return nil, nil, err
		case <-clientErrChan:
			select {
			case <-t.errChan:
				return nil, nil, err
			default:
			}
		}
	}
}

func (t testServer) endServer(tt *testing.T) {

	t.kill <- struct{}{}

	if err := <-t.errChan; err != nil {
		tt.Error(err)
	}

	return
}

func Test_newMessage(t *testing.T) {
	msg := newMessage(10)

	if len(msg) != 10 {
		t.Fatal("message was the incorrect size")
	}

	deleteMessage(msg)

	msg = newMessage(5)

	if len(msg) != 5 {
		t.Fatal("message was the incorrect size")
	}

	deleteMessage(msg)

	msg = newMessage(100)

	if len(msg) != 100 {
		t.Fatal("message was the incorrect size")
	}
}

func Test_readConnection(t *testing.T) {

	t.Run("read write connection", func(t *testing.T) {
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

		alive, err := writeToConnection(serverConn, []byte("hello world"))

		if !alive {
			t.Fatal(err)
		}

		buff := make([]byte, len([]byte("hello world")))

		alive, err = readConnection(clientConn, buff)

		if !alive {
			t.Fatal(err)
		}

		expected := []byte("hello world")

		if !bytes.Equal(expected, buff) {
			t.Fatal("buffer does not match expected")
		}
	})

	t.Run("read cancelled connection", func(t *testing.T) {
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

		err = serverConn.Close()
		if err != nil {
			t.Fatal(err)
		}

		alive, err := readConnection(clientConn, make([]byte, 10))

		if alive {
			t.Fatal("connection did not close")
		}
	})
}

func Test_readMessageLength(t *testing.T) {

	t.Run("test read length", func(t *testing.T) {
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

		mess := make([]byte, 4)
		binary.LittleEndian.PutUint32(mess, 108)

		alive, err := writeToConnection(serverConn, mess)

		if !alive {
			t.Fatal(err)
		}

		length, alive, err := readMessageLength(clientConn)

		if !alive {
			t.Fatal(err)
		}

		if length != 108 {
			t.Fatal("values do not match")
		}
	})
}

func Test_readMessage(t *testing.T) {

	t.Run("test read message", func(t *testing.T) {
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

		mess := "hello this is a test message"

		messLen := make([]byte, 4)
		binary.LittleEndian.PutUint32(messLen, uint32(len(mess)))
		alive, err := writeToConnection(serverConn, messLen)

		if !alive {
			t.Fatal(err)
		}

		buff := []byte(mess)
		alive, err = writeToConnection(serverConn, buff)

		if !alive {
			t.Fatal(err)
		}

		message, alive, err := readMessage(clientConn, time.Second*4)

		if !alive {
			t.Fatal(err)
		}

		if string(message) != mess {
			t.Fatal("messages do not match")
		}
	})

	t.Run("test read over deadline", func(t *testing.T) {
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

		_, alive, err := readMessage(clientConn, time.Second*0)

		if alive {
			t.Fatal("connection did not close")
		}
	})
}

func Test_writeMessage(t *testing.T) {

	t.Run("test write message", func(t *testing.T) {
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

		message := "this is a test message"

		alive, err := writeMessage(clientConn, time.Second*4, []byte(message))

		if !alive {
			t.Fatal(err)
		}

		lengthBuffer := make([]byte, 4)

		alive, err = readConnection(serverConn, lengthBuffer)

		if !alive {
			t.Fatal(err)
		}

		mLen := binary.LittleEndian.Uint32(lengthBuffer)

		if mLen != uint32(len(message)) {
			t.Fatal("sizes do not match")
		}

		buffer := make([]byte, mLen)
		alive, err = readConnection(serverConn, buffer)

		if !alive {
			t.Fatal(err)
		}

		if string(buffer) != message {
			t.Fatal("messages do not match")
		}
	})

	t.Run("test write over deadline", func(t *testing.T) {
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

		message := "this is a test message"

		alive, err := writeMessage(clientConn, time.Second*0, []byte(message))

		if alive {
			t.Fatal("message is alive")
		}
	})
}

func Test_splitResponse(t *testing.T) {
	type test struct {
		name    string
		message []byte
		errs    bool
	}

	tests := []test{
		{
			"test pass",
			[]byte("PASS; T;;;;his test passed"),
			false,
		},
		{
			"test fail",
			[]byte("FAIL;;;; This test;;;;; failed"),
			true,
		},
		{
			"test unknown",
			[]byte("hallo;;;; This test;;;;; failed"),
			true,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			_, err := splitResponse(tst.message)

			if tst.errs && err == nil {
				t.Error("splitResponse did not error when needed")
			}

			if !tst.errs && err != nil {
				t.Error(err)
			}
		})
	}
}
