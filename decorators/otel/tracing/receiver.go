package tracing

import (
	"context"
	"encoding/base64"

	"github.com/zerofox-oss/go-msg"
	"go.opencensus.io/trace/propagation"
	"go.opentelemetry.io/otel"
	ocbridge "go.opentelemetry.io/otel/bridge/opencensus"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("github.com/zerofox-oss/go-msg/decorators/otel")

const (
	traceContextKey = "Tracecontext"
	traceStateKey   = "Tracestate"
)

type Options struct {
	SpanName     string
	StartOptions trace.SpanStartOption
	OnlyOtel     bool
}

type Option func(*Options)

func WithSpanName(spanName string) Option {
	return func(o *Options) {
		o.SpanName = spanName
	}
}

func WithStartOption(so trace.SpanStartOption) Option {
	return func(o *Options) {
		o.StartOptions = so
	}
}

func WithOnlyOtel(onlyOtel bool) Option {
	return func(o *Options) {
		o.OnlyOtel = onlyOtel
	}
}

// Receiver Wraps another msg.Receiver, populating
// the context with any upstream tracing information.
func Receiver(next msg.Receiver, opts ...Option) msg.Receiver {
	options := &Options{
		SpanName: "msg.Receiver",
	}

	for _, opt := range opts {
		opt(options)
	}

	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		ctx, span := withContext(ctx, m, options)
		defer span.End()
		return next.Receive(ctx, m)
	})
}

// withContext checks to see if a traceContext is
// present in the message attributes. If one is present
// a new span is created with that tracecontext as the parent
// otherwise a new span is created without a parent. A new context
// which contains the created span well as the span itself
// is returned
func withContext(ctx context.Context, m *msg.Message, options *Options) (context.Context, trace.Span) {
	textCarrier := msgAttributesTextCarrier{attributes: &m.Attributes}
	tmprop := otel.GetTextMapPropagator()

	// if any of the fields used by
	// the text map propagation is set
	// we use otel to decode
	for _, field := range tmprop.Fields() {
		if m.Attributes.Get(field) != "" {
			ctx = tmprop.Extract(ctx, textCarrier)
			return tracer.Start(ctx, options.SpanName)
		}
	}

	// if we are set to use only otel
	// do not fall back to opencensus
	if options.OnlyOtel {
		return tracer.Start(ctx, options.SpanName)
	}

	// fallback to old behaviour (opencensus) if we don't
	// receive any otel headers
	traceContextB64 := m.Attributes.Get(traceContextKey)
	if traceContextB64 == "" {
		return tracer.Start(ctx, options.SpanName)
	}

	traceContext, err := base64.StdEncoding.DecodeString(traceContextB64)
	if err != nil {
		return tracer.Start(ctx, options.SpanName)
	}

	spanContext, ok := propagation.FromBinary(traceContext)
	if !ok {
		return tracer.Start(ctx, options.SpanName)
	}

	traceStateString := m.Attributes.Get(traceStateKey)
	if traceStateString != "" {
		ts := tracestateFromString(traceStateString)
		spanContext.Tracestate = ts
	}

	// convert the opencensus span context to otel
	otelSpanContext := ocbridge.OCSpanContextToOTel(spanContext)
	if !otelSpanContext.IsValid() {
		return tracer.Start(ctx, options.SpanName)
	}

	return tracer.Start(trace.ContextWithRemoteSpanContext(ctx, otelSpanContext), options.SpanName)
}
