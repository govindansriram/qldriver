package qldriver

import (
	"bytes"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"
	"testing"
)

func Test_splitMessage(t *testing.T) {
	type test struct {
		name    string
		message []byte
		errs    bool
	}

	uid, err := uuid.NewUUID()
	if err != nil {
		t.Fatal(err)
	}

	mess := []byte("greetings;")
	mess = append(mess, uid[:]...)

	tests := []test{
		{
			"test invalid uuid",
			[]byte("greetings; T;;;;his test passed"),
			true,
		},
		{
			"test invalid greeting",
			[]byte("FAIL;;;; This test;;;;; failed"),
			true,
		},
		{
			"test valid greeting and uuid",
			mess,
			false,
		},
	}

	for _, tst := range tests {
		t.Run(tst.name, func(t *testing.T) {
			_, err := splitMessage(tst.message)

			if tst.errs && err == nil {
				t.Error("splitMessage did not error when needed")
			}

			if !tst.errs && err != nil {
				t.Error(err)
			}
		})
	}
}

func Test_getEncodedChallenge(t *testing.T) {

	uid, err := uuid.NewUUID()

	if err != nil {
		t.Fatal(err)
	}

	mess, err := getEncodedChallenge("test", "password", false, uid)

	validPassword := make([]byte, len("password")+16)

	copy(validPassword, "password")
	copy(validPassword[len("password"):], uid[:])

	if err != nil {
		t.Fatal(err)
	}

	if mess[0] != 1 {
		t.Fatal("invalid role")
	}

	mess = mess[1:]

	before, after, found := bytes.Cut(mess, []byte(";"))

	if !found {
		t.Fatal("no splits were found")
	}

	err = bcrypt.CompareHashAndPassword(after, validPassword)

	if err != nil {
		t.Fatal(err)
	}

	if string(before) != "test" {
		t.Fatal("username is invalid")
	}
}
