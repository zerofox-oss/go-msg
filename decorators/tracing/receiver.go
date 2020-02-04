package tracing

import (
	"context"
	"encoding/base64"

	"github.com/zerofox-oss/go-msg"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
)

const traceContextKey = "Tracecontext"
const traceStateKey = "Tracestate"

type Options struct {
	SpanName     string
	StartOptions trace.StartOptions
}

type Option func(*Options)

func WithSpanName(spanName string) Option {
	return func(o *Options) {
		o.SpanName = spanName
	}
}

func WithStartOption(so trace.StartOptions) Option {
	return func(o *Options) {
		o.StartOptions = so
	}
}

// Receiver Wraps another msg.Receiver, populating
// the context with any upstream tracing information.
func Receiver(next msg.Receiver, opts ...Option) msg.Receiver {

	options := &Options{
		SpanName:     "msg.Receiver",
		StartOptions: trace.StartOptions{},
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
func withContext(ctx context.Context, m *msg.Message, options *Options) (context.Context, *trace.Span) {
	traceContextB64 := m.Attributes.Get(traceContextKey)

	startOptions := options.StartOptions

	if traceContextB64 == "" {
		return trace.StartSpan(ctx, options.SpanName, trace.WithSampler(startOptions.Sampler))
	}

	traceContext, err := base64.StdEncoding.DecodeString(traceContextB64)
	if err != nil {
		return trace.StartSpan(ctx, options.SpanName, trace.WithSampler(startOptions.Sampler))
	}

	spanContext, ok := propagation.FromBinary(traceContext)
	if !ok {
		return trace.StartSpan(ctx, options.SpanName, trace.WithSampler(startOptions.Sampler))
	}

	traceStateString := m.Attributes.Get(traceStateKey)
	if traceStateString != "" {
		ts := tracestateFromString(traceStateString)
		spanContext.Tracestate = ts
	}

	return trace.StartSpanWithRemoteParent(ctx, options.SpanName, spanContext, trace.WithSampler(startOptions.Sampler))
}
