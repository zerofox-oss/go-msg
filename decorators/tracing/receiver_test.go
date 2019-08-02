package tracing

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"io/ioutil"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/zerofox-oss/go-msg"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

type msgWithContext struct {
	msg *msg.Message
	ctx context.Context
}

type ChanReceiver struct {
	c chan msgWithContext
}

func (r ChanReceiver) Receive(ctx context.Context, m *msg.Message) error {
	r.c <- msgWithContext{msg: m, ctx: ctx}
	return nil
}

func makeSpanContext() (trace.SpanContext, string) {
	b := make([]byte, 24)
	rand.Read(b)

	var tid [16]byte
	var sid [8]byte

	copy(tid[:], b[:16])
	copy(sid[:], b[:8])

	sc := trace.SpanContext{
		TraceID: tid,
		SpanID:  sid,
	}

	b64 := base64.StdEncoding.EncodeToString(propagation.Binary(sc))
	return sc, b64
}

// Tests that when a Receiver is wrapped by TracingReceiver, and tracecontext
// is present, a span is started and set in the receive context with the correct
// parent context
func TestDecoder_SuccessfullyDecodesSpanWhenTraceContextIsPresent(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan msgWithContext)
	r := Receiver(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc, b64Sc := makeSpanContext()

	// Construct a message with base64 encoding (YWJjMTIz == abc123)
	m := &msg.Message{
		Body:       bytes.NewBufferString("hello"),
		Attributes: msg.Attributes{},
	}
	m.Attributes.Set("Tracecontext", b64Sc)

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan

		expectedBody := "hello"
		actual, _ := ioutil.ReadAll(result.msg.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		span := trace.FromContext(result.ctx)
		if span == nil {
			t.Errorf("span was not expected to be nil")
		}

		receivedSC := span.SpanContext()

		if receivedSC.TraceID != sc.TraceID {
			t.Errorf(cmp.Diff(receivedSC.TraceID, sc.TraceID))
		}

		if receivedSC.Tracestate != sc.Tracestate {
			t.Errorf(cmp.Diff(receivedSC.TraceID, sc.TraceID))
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

// Tests that when a Receiver is wrapped by a Tracing Receiver, and
// the message does not contain a tracecontext, a new span is created
func TestDecoder_SuccessfullySetsSpanWhenNoTraceContext(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan msgWithContext)
	r := Receiver(ChanReceiver{
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
		actual, _ := ioutil.ReadAll(result.msg.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		span := trace.FromContext(result.ctx)
		if span == nil {
			t.Errorf("span was not expected to be nil")
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

// Tests that when a Receiver is wrapped by a Tracing Receiver, and
// the message contains an invalid b64 encodeded tracecontext, a span
// is still sucessfully set
func TestDecoder_SuccessfullySetsSpanWhenInvalidTraceContextB64(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan msgWithContext)
	r := Receiver(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message without base64 encoding
	m := &msg.Message{
		Body:       bytes.NewBufferString("abc123"),
		Attributes: msg.Attributes{},
	}

	m.Attributes.Set("Tracecontext", "invalidcontext")

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan
		expectedBody := "abc123"
		actual, _ := ioutil.ReadAll(result.msg.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		span := trace.FromContext(result.ctx)
		if span == nil {
			t.Errorf("span was not expected to be nil")
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

// Tests that when a Receiver is wrapped by a Tracing Receiver, and
// the message contains an invalid binary encodeded tracecontext, a span
// is still sucessfully set
func TestDecoder_SuccessfullySetsSpanWhenInvalidTraceContextBinary(t *testing.T) {
	testFinish := make(chan struct{})
	msgChan := make(chan msgWithContext)
	r := Receiver(ChanReceiver{
		c: msgChan,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Construct a message without base64 encoding
	m := &msg.Message{
		Body:       bytes.NewBufferString("abc123"),
		Attributes: msg.Attributes{},
	}

	// "YWJjMTIz" is valid b64
	m.Attributes.Set("Tracecontext", "YWJjMTIz")

	// Wait for ChanReceiver to write the message to msgChan, assert on the body
	go func() {
		result := <-msgChan
		expectedBody := "abc123"
		actual, _ := ioutil.ReadAll(result.msg.Body)
		if string(actual) != expectedBody {
			t.Errorf("Expected Body to be %v, got %v", expectedBody, string(actual))
		}

		span := trace.FromContext(result.ctx)
		if span == nil {
			t.Errorf("span was not expected to be nil")
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
