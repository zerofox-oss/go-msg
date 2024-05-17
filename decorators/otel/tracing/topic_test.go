package tracing

import (
	"context"
	"encoding/base64"
	"testing"

	msg "github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	ocprop "go.opencensus.io/trace/propagation"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTopic__SucessfullyInsertsTraceContext(t *testing.T) {
	tp := tracesdk.NewTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		tp.Shutdown(context.Background())
	})
	c := make(chan *msg.Message, 2)

	// setup topics
	t2 := Topic(&mem.Topic{C: c}, WithSpanName("something.Different"))

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

	_, ok := ocprop.FromBinary(b)
	if !ok {
		t.Errorf("expected spanContext to be decoded from tracecontext attribute")
	}
}

func TestTopic__SucessfullyInsertsOtelContext(t *testing.T) {
	tp := tracesdk.NewTracerProvider()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		tp.Shutdown(context.Background())
	})
	c := make(chan *msg.Message, 2)

	// setup topics
	t2 := Topic(&mem.Topic{C: c}, WithSpanName("something.Different"))

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

	traceparent := m.Attributes.Get("Traceparent")
	if traceparent == "" {
		t.Fatalf("expected traceparent attribute to be set")
	}

	textMapProp := otel.GetTextMapPropagator()
	ctx := textMapProp.Extract(context.Background(), msgAttributesTextCarrier{attributes: &m.Attributes})
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		t.Fatalf("expected otel span to be valid")
	}

	if !span.SpanContext().IsRemote() {
		t.Fatalf("expected otel span to be remote")
	}
}
