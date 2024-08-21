package qldriver

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"
)

const maxIoTime = uint16(5)
const address = "localhost"
const port = uint16(8080)

func startQlite() (*exec.Cmd, io.ReadCloser, error) {

	cmd := exec.Command("qlite", "start", "config.yaml")
	stdout, err := cmd.StderrPipe()

	if err != nil {
		return nil, nil, err
	}

	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}

	//init time
	time.Sleep(time.Second * 1)

	return cmd, stdout, nil
}

func stopQlite(cmd *exec.Cmd) error {
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		return err
	}

	return nil
}

func qliteLogs(stdout io.ReadCloser) {
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		fmt.Println(scanner.Text())
	}
}

func continuousPush(kChan chan struct{}, driver PublisherClient, sleepDuration time.Duration) error {
	count := 0
	for {
		count++
		select {
		case <-kChan:
			fmt.Println(count, "push")
			return nil
		default:
			_, err := driver.Push(make([]byte, 1000))

			if err != nil {
				return err
			}

			time.Sleep(sleepDuration)
		}
	}
}

func continuousPop(kChan chan struct{}, driver SubscriberClient, sleepDuration time.Duration) {
	count := 0

	for {
		select {
		case <-kChan:
			fmt.Println(count)
			return
		default:
			count++
			if count%2 == 0 {
				uid, _, err := driver.Hide(2)

				//fmt.Println(count)

				if err != nil {
					//fmt.Println("failed")
					continue
				}

				_, _ = driver.Delete(uid)
			} else {
				_, _, _ = driver.Poll(1)
			}

			time.Sleep(sleepDuration)
		}
	}
}

func TestPublisherClient_Push(t *testing.T) {
	t.Run("single push", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		pos, err := pub.Push(make([]byte, 1000))

		if err != nil {
			t.Error(err)
		}

		if pos != 1 {
			t.Fatal("position is invalid")
		}
	})

	t.Run("continuous concurrent pushes", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		kill := make(chan struct{})

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		errSlice := make([]error, 5)
		wg := sync.WaitGroup{}

		for index := range errSlice {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := continuousPush(kill, pub, time.Second*0)
				errSlice[index] = err
			}()
		}

		duration := time.Second * 3
		time.Sleep(duration)

		close(kill)
		wg.Wait()

		for _, i := range errSlice {
			if i != nil {
				t.Error(i)
			}
		}
	})
}

func TestSubscriberClient_Hide(t *testing.T) {
	t.Run("single hide", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		message := []byte("hello world")

		_, err = pub.Push(message)

		if err != nil {
			t.Error(err)
		}

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		_, mess, err := sub.Hide(10)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal([]byte("hello world"), mess) {
			t.Fatal("received incorrect message")
		}
	})

	t.Run("pop from empty", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		_, _, err = sub.Hide(10)

		if err == nil {
			t.Fatal("popped from empty queue")
		}
	})
}

func TestSubscriberClient_Poll(t *testing.T) {
	t.Run("single poll", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		go func() {
			time.Sleep(3 * time.Second)
			message := []byte("hello world")

			_, err = pub.Push(message)

			if err != nil {
				t.Error(err)
			}
		}()

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		_, mess, err := sub.Poll(10)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal([]byte("hello world"), mess) {
			t.Fatal("received incorrect message")
		}
	})

	t.Run("pop from empty", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		_, _, err = sub.Poll(10)

		if err == nil {
			t.Fatal("popped from empty queue")
		}
	})
}

func TestSubscriberClient_Delete(t *testing.T) {
	t.Run("single delete", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		message := []byte("hello world")

		_, err = pub.Push(message)

		if err != nil {
			t.Fatal(err)
		}

		l, err := pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		if l != 1 {
			t.Fatal(err)
		}

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		uid, mess, err := sub.Hide(10)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal([]byte("hello world"), mess) {
			t.Fatal("received incorrect message")
		}

		uid, err = sub.Delete(uid)

		if err != nil {
			t.Fatal(err)
		}

		l, err = pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		if l != 0 {
			t.Fatal(err)
		}
	})

	t.Run("repeated delete", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		message := []byte("hello world")

		_, err = pub.Push(message)

		if err != nil {
			t.Fatal(err)
		}

		l, err := pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		if l != 1 {
			t.Fatal(err)
		}

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		uid, mess, err := sub.Hide(10)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal([]byte("hello world"), mess) {
			t.Fatal("received incorrect message")
		}

		uid, err = sub.Delete(uid)

		if err != nil {
			t.Fatal(err)
		}

		l, err = pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		if l != 0 {
			t.Fatal(err)
		}

		uid, err = sub.Delete(uid)

		if err == nil {
			t.Fatal("second delete should have failed")
		}
	})

	t.Run("expired delete", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		message := []byte("hello world")

		_, err = pub.Push(message)

		if err != nil {
			t.Fatal(err)
		}

		l, err := pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		if l != 1 {
			t.Fatal(err)
		}

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		uid, mess, err := sub.Hide(0)

		if err != nil {
			t.Fatal(err)
		}

		if !bytes.Equal([]byte("hello world"), mess) {
			t.Fatal("received incorrect message")
		}

		time.Sleep(time.Second * 1)

		uid, err = sub.Delete(uid)

		if err == nil {
			t.Fatal("delete should have failed")
		}
	})
}

func Test_ALL(t *testing.T) {
	t.Run("single delete", func(t *testing.T) {
		cmd, _, err := startQlite()

		if err != nil {
			t.Fatal(err)
		}

		kill := make(chan struct{})

		pub := NewPublisherClient(
			"pub",
			"publisher",
			10,
			maxIoTime,
			port,
			address)

		defer func() {
			err := stopQlite(cmd)
			if err != nil {
				t.Fatal(err)
			}
		}()

		sub := NewSubscriberClient(
			"sub",
			"subscriber",
			5,
			maxIoTime,
			10,
			port,
			address)

		errSlice := make([]error, 10)
		wg := sync.WaitGroup{}

		for index := range errSlice {
			wg.Add(1)
			go continuousPop(kill, sub, time.Second*0)
			go func() {
				defer wg.Done()
				err := continuousPush(kill, pub, time.Second*0)
				errSlice[index] = err
			}()
		}

		duration := time.Second * 3
		time.Sleep(duration)

		close(kill)
		wg.Wait()

		for _, i := range errSlice {
			if i != nil {
				t.Error(i)
			}
		}

		l, err := pub.Len()

		if err != nil {
			t.Fatal(err)
		}

		fmt.Println(l)
	})
}
