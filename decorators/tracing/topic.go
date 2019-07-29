package tracing

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

	"github.com/zerofox-oss/go-msg"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

func msgAttributesToTrace(msgAttributes msg.Attributes) []trace.Attribute {
	traceAttributes := make([]trace.Attribute, len(msgAttributes))
	for key, value := range msgAttributes {
		traceAttributes = append(traceAttributes, trace.StringAttribute(key, strings.Join(value, ";")))
	}
	return traceAttributes
}

// Topic wraps a msg.Topic, attaching any tracing data
// via msg.Attributes to send downstream
func Topic(next msg.Topic, opts ...Option) msg.Topic {

	options := &Options{
		SpanName: "msg.MessageWriter",
		StartOptions: trace.StartOptions{
			Sampler: trace.AlwaysSample(),
		},
	}

	for _, opt := range opts {
		opt(options)
	}

	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		tracingCtx, span := trace.StartSpan(
			ctx,
			options.SpanName,
			trace.WithSampler(options.StartOptions.Sampler),
		)

		return &tracingWriter{
			Next:    next.NewWriter(ctx),
			ctx:     tracingCtx,
			onClose: span.End,
			options: options,
		}
	})
}

type tracingWriter struct {
	Next msg.MessageWriter

	buf    bytes.Buffer
	closed bool
	mux    sync.Mutex
	ctx    context.Context

	// onClose is used to end the span
	// and send data to tracing framework
	onClose func()

	options *Options
}

// Attributes returns the attributes associated with the MessageWriter.
func (w *tracingWriter) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}

// Close adds tracing message attributes
// writing to the next MessageWriter.
func (w *tracingWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()
	defer w.onClose()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	dataToWrite := w.buf.Bytes()

	if span := trace.FromContext(w.ctx); span != nil {
		sc := span.SpanContext()
		bs := propagation.Binary(sc)
		traceBuf := make([]byte, base64.StdEncoding.EncodedLen(len(bs)))
		base64.StdEncoding.Encode(traceBuf, bs)

		attrs := *w.Attributes()

		span.AddAttributes(msgAttributesToTrace(attrs)...)

		attrs.Set(traceContextKey, string(traceBuf))
		span.Annotate([]trace.Attribute{}, fmt.Sprintf("%q", dataToWrite))

	}

	if _, err := w.Next.Write(dataToWrite); err != nil {
		return err
	}
	return w.Next.Close()
}

// Write writes bytes to an internal buffer.
func (w *tracingWriter) Write(b []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(b)
}
