package lz4

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
// is decoded when the Content-Compression header is set to lz4.
func TestDecoder_SuccessfullyDecodesWhenHeaderIsSet(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan *msg.Message)
	r := Decoder(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message with lz4 compressed
	m := &msg.Message{
		Body:       bytes.NewBuffer([]byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x29, 0x0, 0x0, 0x0, 0x11, 0x61, 0x1, 0x0, 0xef, 0x62, 0x62, 0x62, 0x62, 0x62, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x31, 0x32, 0x33, 0x14, 0x0, 0x1, 0x0, 0x14, 0x0, 0x0, 0x28, 0x0, 0xc0, 0x62, 0x62, 0x62, 0x63, 0x63, 0x63, 0x63, 0x63, 0x63, 0x31, 0x32, 0x33, 0x0, 0x0, 0x0, 0x0, 0x30, 0x28, 0x73, 0x84}),
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Encoding", "lz4")

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan

		expectedBody := "aaaaaabbbbbcccccc123aaaaaabbbbbcccccc123aaaaaabbbbbcccccc123"
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
// is not changed if Content-Compression is not set to lz4.
func TestDecoder_DoesNotModifyMessageWithoutAppropriateHeader(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan *msg.Message)
	r := Decoder(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message without lz4 encoding
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

// Tests that isLz4Compressed properly identifies Content-Compression
func TestIsLz4Compressed_True(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Encoding", "lz4")

	if !isLz4Compressed(m) {
		t.Error("Expected m to be lz4 compressed but got false.")
	}
}

// Tests that isLz4Compressed returns false if Content-Compression is
// set to a value other than lz4.
func TestIsLz4Compressed_FalseOtherValueInContentCompression(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Encoding", "gzip")

	if isLz4Compressed(m) {
		t.Error("Expected m not to be lz4 compressed but got true.")
	}
}

// Tests that isLz4Compressed returns false if Content-Compression is
// not set.
func TestIsLz4Compressed_FalseContentCompressionNotSet(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Other-Header", "lz4")

	if isLz4Compressed(m) {
		t.Error("Expected m not to be lz4 compressed but got true.")
	}
}
