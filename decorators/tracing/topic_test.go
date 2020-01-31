package tracing

import (
	"context"
	"encoding/base64"
	"testing"

	msg "github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"go.opencensus.io/trace/tracestate"
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

func TestTopic__SucessfullyInsertsTraceContextWithTraceState(t *testing.T) {
	c := make(chan *msg.Message, 2)

	// setup topics
	t1 := mem.Topic{C: c}
	t2 := Topic(&t1, WithSpanName("something.Different"))

	pctx, pspan := trace.StartSpan(context.Background(), "PSpan")

	ts, err := tracestate.New(nil, tracestate.Entry{Key: "debug", Value: "true"})
	if err != nil {
		t.Fatal(err)
	}

	psc := trace.SpanContext{
		TraceID:      pspan.SpanContext().TraceID,
		SpanID:       pspan.SpanContext().SpanID,
		TraceOptions: pspan.SpanContext().TraceOptions,
		Tracestate:   ts,
	}

	ctx, _ := trace.StartSpanWithRemoteParent(pctx, "SpanName", psc)

	w := t2.NewWriter(ctx)
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

	tsstring := m.Attributes.Get("Tracestate")
	if tsstring != "debug=true" {
		t.Fatalf("expected tracecontext attribute to be set")
	}
}
