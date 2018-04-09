package base64

import (
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

	encodedMsg := "aGVsbG8sd29ybGQh"
	if string(body) != encodedMsg {
		t.Fatalf("got %s expected %s", string(body), encodedMsg)
	}
}

// Tests that an base64 MessageWriter can be only be used once
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

	encodedMsg := "ZG9udCB0cnkgdG8gdXNlIHRoaXMgdHdpY2Uh"
	if string(body) != encodedMsg {
		t.Fatalf("got %s expected %s", string(body), encodedMsg)
	}

	if _, err := w.Write([]byte("this will fail!!!")); err != msg.ErrClosedMessageWriter {
		t.Errorf("expected ErrClosedMessageWriter, got %v", err)
	}
}
