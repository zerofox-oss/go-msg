package lz4

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
)

type ChanReceiver struct {
	c chan *msg.Message
}

func (r ChanReceiver) Receive(ctx context.Context, m *msg.Message) error {
	r.c <- m
	return nil
}

// Tests that Compressor and Decompressor can be used in combination
// to successfully process a message (black-box integration test).
func TestCompressorDecompressor_SuccessEndToEnd(t *testing.T) {
	testFinish := make(chan struct{})
	tChan := make(chan *msg.Message)
	rChan := make(chan *msg.Message)

	// setup topics
	t1 := mem.Topic{C: tChan}
	t2 := Compressor(&t1)

	// setup receiver
	r := Decompressor(ChanReceiver{
		c: rChan,
	})

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		m := <-rChan

		// assert that message body is same as original input message
		expectedBody := "hello, world!"
		actual, _ := ioutil.ReadAll(m.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be '%v', got '%v'", expectedBody, string(actual))
		}

		testFinish <- struct{}{}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for topic chan to receive a message
	go func() {
		m := <-tChan

		// assert that Receive processes message with no errors
		err := r.Receive(ctx, m)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	// write message to topic to kick off test flow
	w := t2.NewWriter(context.Background())
	w.Write([]byte("hello, world!"))
	w.Close()

	<-testFinish
}

// Tests that Decompressor does nothing if Content-Encoding header has the wrong value
func TestCompressorDecompressor_DoesNotDecompressIfContentEncodingHeaderIncorrect(t *testing.T) {
	testFinish := make(chan struct{})
	tChan := make(chan *msg.Message)
	rChan := make(chan *msg.Message)

	// setup topics
	t1 := mem.Topic{C: tChan}
	t2 := Compressor(&t1)

	// setup receiver
	r := Decompressor(ChanReceiver{
		c: rChan,
	})

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		m := <-rChan

		// assert that message body is different from original input message
		notExpectedBody := "hello, world!"
		actual, _ := ioutil.ReadAll(m.Body)
		if string(actual) == notExpectedBody {
			t.Errorf("Expected Body to NOT be '%v', got '%v'", notExpectedBody, string(actual))
		}

		testFinish <- struct{}{}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Wait for topic chan to receive a message
	go func() {
		m := <-tChan

		// Set Content-Encoding to the wrong value
		m.Attributes.Set("Content-Encoding", "gzip")

		// assert that Receive processes message with no errors
		err := r.Receive(ctx, m)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	// write message to topic to kick off test flow
	w := t2.NewWriter(context.Background())
	w.Write([]byte("hello, world!"))
	w.Close()

	<-testFinish
}

// Tests that isLZ4Compressed properly identifies Content-Encoding.
func TestIsLZ4Compressed_True(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Encoding", "lz4")

	if !isLZ4Compressed(m) {
		t.Error("Expected m to be lz4 compressed but got false.")
	}
}

// Tests that isLZ4Compressed returns false if Content-Encoding is
// set to a value other than lz4.
func TestIsLZ4Compressed_FalseOtherValueInContentEncoding(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Content-Encoding", "gzip")

	if isLZ4Compressed(m) {
		t.Error("Expected m not to be lz4 compressed but got true.")
	}
}

// Tests that isLZ4Compressed returns false if Content-Encoding is
// not set.
func TestIsLZ4Compressed_FalseContentEncodingNotSet(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Other-Header", "lz4")

	if isLZ4Compressed(m) {
		t.Error("Expected m not to be lz4 compressed but got true.")
	}
}
