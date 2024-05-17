package tracing

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"sync"

	"github.com/zerofox-oss/go-msg"
	"go.opencensus.io/trace/propagation"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	ocbridge "go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/trace"
)

func msgAttributesToTrace(msgAttributes msg.Attributes) []attribute.KeyValue {
	traceAttributes := make([]attribute.KeyValue, len(msgAttributes))
	for key, value := range msgAttributes {
		traceAttributes = append(traceAttributes, attribute.String(key, strings.Join(value, ";")))
	}
	return traceAttributes
}

type msgAttributesTextCarrier struct {
	attributes *msg.Attributes
}

func (m msgAttributesTextCarrier) Get(key string) string {
	return m.attributes.Get(key)
}

func (m msgAttributesTextCarrier) Set(key string, value string) {
	m.attributes.Set(key, value)
}

func (m msgAttributesTextCarrier) Keys() []string {
	keys := []string{}
	for key := range *m.attributes {
		keys = append(keys, key)
	}
	return keys
}

// Topic wraps a msg.Topic, attaching any tracing data
// via msg.Attributes to send downstream
func Topic(next msg.Topic, opts ...Option) msg.Topic {
	options := &Options{
		SpanName: "msg.MessageWriter",
	}

	for _, opt := range opts {
		opt(options)
	}

	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		tracingCtx, span := tracer.Start(
			ctx,
			options.SpanName,
			trace.WithSpanKind(trace.SpanKindProducer),
		)

		return &tracingWriter{
			Next:    next.NewWriter(tracingCtx),
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
	onClose func(...trace.SpanEndOption)

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

	if span := trace.SpanFromContext(w.ctx); span != nil {
		// set message attributes
		// as span tags for debugging
		attrs := *w.Attributes()
		span.SetAttributes(msgAttributesToTrace(attrs)...)

		// we use the global text map propagator
		// to set string values onto the message
		// attributes, this will set the headers
		// in the new style tracecontext format
		textCarrier := msgAttributesTextCarrier{attributes: w.Attributes()}
		tmprop := otel.GetTextMapPropagator()
		tmprop.Inject(w.ctx, textCarrier)

		// also send opencensus headers
		// for backwards compatiblity
		if !w.options.OnlyOtel {
			// we need to convert the otel
			// span to the old style opencensus
			sc := span.SpanContext()
			ocSpan := ocbridge.OTelSpanContextToOC(sc)
			bs := propagation.Binary(ocSpan)
			traceBuf := make([]byte, base64.StdEncoding.EncodedLen(len(bs)))
			base64.StdEncoding.Encode(traceBuf, bs)

			attrs.Set(traceContextKey, string(traceBuf))
			tracestateString := tracestateToString(ocSpan)
			if tracestateString != "" {
				attrs.Set(traceStateKey, tracestateToString(ocSpan))
			}
		}
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
