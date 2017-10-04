package mem_test

import (
	"context"
	"testing"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/mem"
)

func TestMessageWriter_Attributes(t *testing.T) {
	testTopic := &mem.Topic{}

	w := testTopic.NewWriter(context.Background())
	attrs := w.Attributes()
	attrs.Set("test", "value")

	if attrs.Get("test") != "value" {
		t.Errorf("expected attribute to be value, got %v", attrs.Get("test"))
	}
}

func TestMessageWriter_WriteAndClose(t *testing.T) {
	channel := make(chan *msg.Message)
	testTopic := &mem.Topic{
		C: channel,
	}

	go func() {
		w := testTopic.NewWriter(context.Background())
		w.Write([]byte("Don't "))
		w.Write([]byte("call me "))
		w.Write([]byte("junior!"))
		w.Close()
	}()

	m := <-channel

	body, err := msg.DumpBody(m)
	if err != nil {
		t.Fatalf("could not read bytes from body: %s", err.Error())
	}

	expected := []byte("Don't call me junior!")
	if string(body) != string(expected) {
		t.Errorf("expected %s got %s", string(expected), string(body))
	}
}

// asserts MessageWriter can only be used once
func TestMesageWriter_SingleUse(t *testing.T) {
	channel := make(chan *msg.Message)
	testTopic := &mem.Topic{
		C: channel,
	}

	w := testTopic.NewWriter(context.Background())

	text := [][]byte{
		[]byte("I have a bad feeling about this..."),
		[]byte("Great shot kid that was one in a million!"),
	}

	go func() {
		if _, err := w.Write(text[0]); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		// second write, close should fail
		if _, err := w.Write(text[1]); err != msg.ErrClosedMessageWriter {
			t.Errorf("expected %v, got %v", msg.ErrClosedMessageWriter, err)
		}
		if err := w.Close(); err != msg.ErrClosedMessageWriter {
			t.Errorf("expected %v, got %v", msg.ErrClosedMessageWriter, err)
		}
	}()

	m := <-channel

	body, err := msg.DumpBody(m)
	if err != nil {
		t.Fatalf("could not read bytes from body: %s", err.Error())
	}

	if string(body) != string(text[0]) {
		t.Errorf("expected %s got %s", string(text[0]), string(body))
	}
}
