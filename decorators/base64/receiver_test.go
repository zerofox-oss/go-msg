package base64

import (
	"bytes"
	"context"
	"io/ioutil"
	"testing"

	"github.com/zerofox-oss/go-msg"
)

type ChanReceiver struct {
	c chan *msg.Message
}

func (r ChanReceiver) Receive(ctx context.Context, m *msg.Message) error {
	r.c <- m
	return nil
}

// Tests that when a Receiver is wrapped by Decoder, the body of a message
// is base64 decoded when the Content-Transfer-Encoding header is set to base64.
func TestDecoder_SuccessfullyDecodesWhenHeaderIsSet(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan *msg.Message)
	r := Decoder(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message with base64 encoding (YWJjMTIz == abc123)
	m := &msg.Message{
		Body:       bytes.NewBufferString("YWJjMTIz"),
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Transfer-Encoding", "base64")

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan

		expectedBody := "abc123"
		actual, _ := ioutil.ReadAll(result.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		testFinish <- struct{}{}
	}()

	// Receive the message!
	err := r.Receive(ctx, m)
	if err != nil {
		t.Error(err)
		return
	}
	<-testFinish
}

// Tests that when a Receiver is wrapped by Decoder, the body of a message
// is not changed if Content-Transfer-Encoding is not set to base64.
func TestDecoder_DoesNotModifyMessageWithoutAppropriateHeader(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan *msg.Message)
	r := Decoder(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message without base64 encoding
	m := &msg.Message{
		Body:       bytes.NewBufferString("abc123"),
		Attributes: msg.Attributes{},
	}

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan

		expectedBody := "abc123"
		actual, _ := ioutil.ReadAll(result.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		testFinish <- struct{}{}
	}()

	// Receive the message!
	err := r.Receive(ctx, m)
	if err != nil {
		t.Error(err)
		return
	}
	<-testFinish
}

// Tests that isBase64Encoded properly identifies Content-Transfer-Encoding.
func TestIsBase64Encoded_True(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Transfer-Encoding", "base64")

	if !isBase64Encoded(m) {
		t.Error("Expected m to be base64 encoded but got false.")
	}
}

// Tests that isBase64Encoded returns false if Content-Transfer-Encoding is
// set to a value other than base64.
func TestIsBase64Encoded_FalseOtherValueInContentTransfer(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Transfer-Encoding", "base65")

	if isBase64Encoded(m) {
		t.Error("Expected m not to be base64 encoded but got true.")
	}
}

// Tests that isBase64Encoded returns false if Content-Transfer-Encoding is
// not set.
func TestIsBase64Encoded_FalseContentTransferNotSet(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Other-Header", "base64")

	if isBase64Encoded(m) {
		t.Error("Expected m not to be base64 encoded but got true.")
	}
}
