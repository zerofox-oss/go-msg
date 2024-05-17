package tracing_test

import (
	"context"
	"sync"
	"testing"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	"github.com/zerofox-oss/go-msg/decorators/otel/tracing"
	octracing "github.com/zerofox-oss/go-msg/decorators/tracing"
	octrace "go.opencensus.io/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type message struct {
	ctx context.Context
	m   *msg.Message
}

func startOTSpan() (context.Context, string, func()) {
	ctx := context.Background()
	ctx, span := otel.Tracer("tracing_test").Start(ctx, "WriteMsg")
	return ctx, span.SpanContext().TraceID().String(), func() {
		span.End()
	}
}

func startOCSpan() (context.Context, string, func()) {
	ctx := context.Background()
	ctx, span := octrace.StartSpan(ctx, "WriteMsg")
	return ctx, span.SpanContext().TraceID.String(), func() {
		span.End()
	}
}

func readOCSpan(ctx context.Context) string {
	span := octrace.FromContext(ctx)
	if span != nil {
		return span.SpanContext().TraceID.String()
	}
	return ""
}

func readOTSpan(ctx context.Context) string {
	receivedSpan := trace.SpanFromContext(ctx)
	receivedSpanContext := receivedSpan.SpanContext()
	return receivedSpanContext.TraceID().String()
}

func verifyEncodeDecode(
	t *testing.T,
	topicDecorator func(next msg.Topic) msg.Topic,
	receiveDecorator func(next msg.Receiver) msg.Receiver,
	spanCreator func() (context.Context, string, func()),
	spanReader func(context.Context) string,
) {
	c1 := make(chan *msg.Message)
	topic := mem.Topic{C: c1}
	srv := mem.NewServer(c1, 1)

	var lock sync.RWMutex

	receivedMessages := []message{}
	go func() {
		srv.Serve(receiveDecorator(msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			lock.Lock()
			receivedMessages = append(receivedMessages, message{ctx: ctx, m: m})
			lock.Unlock()
			return nil
		})))
	}()

	expectedTraces := []string{}
	for i := 0; i < 20; i++ {
		ctx, traceID, end := spanCreator()
		writer := topicDecorator(&topic).NewWriter(ctx)
		writer.Write([]byte("message body"))
		writer.Close()
		end()
		expectedTraces = append(expectedTraces, traceID)
	}

	srv.Shutdown(context.Background())

	for i, expectedTraceID := range expectedTraces {
		lock.RLock()
		m := receivedMessages[i]
		lock.RUnlock()
		traceID := spanReader(m.ctx)
		if traceID != expectedTraceID {
			t.Errorf("received span did not have the expected id %s != %s", traceID, expectedTraceID)
		}
	}
}

func TestTopicAndReceiverCompatability(t *testing.T) {
	tp := tracesdk.NewTracerProvider()

	otel.SetTextMapPropagator(propagation.TraceContext{})
	otel.SetTracerProvider(tp)

	t.Cleanup(func() {
		tp.Shutdown(context.Background())
	})

	t.Run("Otel->Otel WithOnlyOtel=true", func(t *testing.T) {
		verifyEncodeDecode(t,
			func(next msg.Topic) msg.Topic {
				return tracing.Topic(next, tracing.WithOnlyOtel(true))
			},
			func(next msg.Receiver) msg.Receiver {
				return tracing.Receiver(next, tracing.WithOnlyOtel(true))
			},
			startOTSpan,
			readOTSpan,
		)
	})

	t.Run("Otel->Otel WithOnlyOtel=false", func(t *testing.T) {
		verifyEncodeDecode(t,
			func(next msg.Topic) msg.Topic {
				return tracing.Topic(next, tracing.WithOnlyOtel(false))
			},
			func(next msg.Receiver) msg.Receiver {
				return tracing.Receiver(next, tracing.WithOnlyOtel(false))
			},
			startOTSpan,
			readOTSpan,
		)
	})

	t.Run("OC->Otel", func(t *testing.T) {
		verifyEncodeDecode(t,
			func(next msg.Topic) msg.Topic {
				return octracing.Topic(next)
			},
			func(next msg.Receiver) msg.Receiver {
				return tracing.Receiver(next, tracing.WithOnlyOtel(false))
			},
			startOCSpan,
			readOTSpan,
		)
	})

	t.Run("Otel->OC", func(t *testing.T) {
		verifyEncodeDecode(t,
			func(next msg.Topic) msg.Topic {
				return tracing.Topic(next, tracing.WithOnlyOtel(false))
			},
			func(next msg.Receiver) msg.Receiver {
				return octracing.Receiver(next)
			},
			startOTSpan,
			readOCSpan,
		)
	})
}
