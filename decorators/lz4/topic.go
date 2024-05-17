package lz4

import (
	"context"
	"sync"

	"github.com/pierrec/lz4/v4"
	"github.com/zerofox-oss/go-msg"
)

// Encoder wraps a topic with another which lz4-encodes a Message.
// This should used in conjunction with the base64 encoder
// if the underlying message queue does not support binary (eg SQS)
func Encoder(next msg.Topic) msg.Topic {
	options := []lz4.Option{
		lz4.CompressionLevelOption(lz4.CompressionLevel(lz4.Level3)),
	}

	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		nextW := next.NewWriter(ctx)
		writer := lz4.NewWriter(nextW)
		err := writer.Apply(options...)
		if err != nil {
			panic(err)
		}
		return &encodeWriter{
			Next:   nextW,
			Writer: writer,
		}
	})
}

type encodeWriter struct {
	Next msg.MessageWriter

	Writer *lz4.Writer
	closed bool
	mux    sync.Mutex
}

// Attributes returns the attributes associated with the MessageWriter.
func (w *encodeWriter) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}

// Close calls Close on the lz4 writer before
// writing bytes to the next MessageWriter.
func (w *encodeWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	attrs := *w.Attributes()
	attrs["Content-Encoding"] = []string{"lz4"}

	if err := w.Writer.Close(); err != nil {
		return err
	}

	return w.Next.Close()
}

// Write writes bytes to lz4 Writer
func (w *encodeWriter) Write(b []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.Writer.Write(b)
}
