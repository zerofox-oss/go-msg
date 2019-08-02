package tracing

import (
	"context"
	"encoding/base64"
	"testing"

	msg "github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	"go.opencensus.io/trace/propagation"
)

func TestTopic__SucessfullyInsertsTraceContext(t *testing.T) {
	c := make(chan *msg.Message, 2)

	// setup topics
	t1 := mem.Topic{C: c}
	t2 := Topic(&t1, WithSpanName("something.Different"))

	w := t2.NewWriter(context.Background())
	w.Write([]byte("hello,"))
	w.Write([]byte("world!"))
	w.Close()

	m := <-c
	body, err := msg.DumpBody(m)
	if err != nil {
		t.Fatal(err)
	}

	expectedBody := "hello,world!"
	if string(body) != expectedBody {
		t.Fatalf("got %s expected %s", string(body), expectedBody)
	}

	tc := m.Attributes.Get("Tracecontext")
	if tc == "" {
		t.Fatalf("expected tracecontext attribute to be set")
	}

	b, err := base64.StdEncoding.DecodeString(tc)
	if err != nil {
		t.Error(err)
	}

	_, ok := propagation.FromBinary(b)
	if !ok {
		t.Errorf("expected spanContext to be decoded from tracecontext attribute")
	}
}
