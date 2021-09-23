package lz4

import (
	"bytes"
	"context"
	"testing"

	msg "github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
)

func TestEncoder(t *testing.T) {
	c := make(chan *msg.Message, 2)

	// setup topics
	t1 := mem.Topic{C: c}
	t2 := Encoder(&t1)

	w := t2.NewWriter(context.Background())
	w.Write([]byte("hello,"))
	w.Write([]byte("world!"))
	w.Close()

	m := <-c
	body, err := msg.DumpBody(m)
	if err != nil {
		t.Fatal(err)
	}

	encodedMsg := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0xc, 0x0, 0x0, 0x80, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x77, 0x6f, 0x72, 0x6c, 0x64, 0x21, 0x0, 0x0, 0x0, 0x0, 0xef, 0x1f, 0xf7, 0xe4}
	if bytes.Compare(body, encodedMsg) != 0 {
		t.Fatalf("got %#v expected %#v", body, encodedMsg)
	}
}

// Tests that an lz4 MessageWriter can be only be used once
func TestEncoder_SingleUse(t *testing.T) {
	c := make(chan *msg.Message, 2)

	// setup topics
	t1 := mem.Topic{C: c}
	t2 := Encoder(&t1)

	w := t2.NewWriter(context.Background())
	w.Write([]byte("dont try to use this twice!"))
	w.Close()

	m := <-c
	body, err := msg.DumpBody(m)
	if err != nil {
		t.Fatal(err)
	}

	encodedMsg := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x1b, 0x0, 0x0, 0x80, 0x64, 0x6f, 0x6e, 0x74, 0x20, 0x74, 0x72, 0x79, 0x20, 0x74, 0x6f, 0x20, 0x75, 0x73, 0x65, 0x20, 0x74, 0x68, 0x69, 0x73, 0x20, 0x74, 0x77, 0x69, 0x63, 0x65, 0x21, 0x0, 0x0, 0x0, 0x0, 0xa1, 0x89, 0xb0, 0xd9}
	if bytes.Compare(body, encodedMsg) != 0 {
		t.Fatalf("got %#v expected %#v", body, encodedMsg)
	}

	if _, err := w.Write([]byte("this will fail!!!")); err != msg.ErrClosedMessageWriter {
		t.Errorf("expected ErrClosedMessageWriter, got %v", err)
	}
}
