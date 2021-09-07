package lz4

import (
	"bytes"
	"context"
	"io"
	"log"
	"sync"

	lz4algo "github.com/pierrec/lz4"
	"github.com/zerofox-oss/go-msg"
)

// Compressor wraps a topic with another that compresses messages
// before writing, using the LZ4 compression algorithm,
// which is well-suited for efficiently compressing high throughput
// data streams.
func Compressor(next msg.Topic) msg.Topic {
	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		return &writer{
			Next: next.NewWriter(ctx),
		}
	})
}

type writer struct {
	Next msg.MessageWriter

	buf    bytes.Buffer
	closed bool
	mux    sync.Mutex
}

// Attributes returns the attributes associated with the MessageWriter.
func (w *writer) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}

// Close lz4-compresses the contents of the buffer before
// writing them to the next MessageWriter.
func (w *writer) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	attrs := *w.Attributes()
	attrs["Content-Encoding"] = []string{"lz4"}

	pipeReader, pipeWriter := io.Pipe()
	lz4Writer := lz4algo.NewWriter(pipeWriter)

	go func() {
		// compress the message body and write to pipe
		_, err := io.Copy(lz4Writer, &w.buf)
		if err != nil {
			log.Println("[ERROR] failed to compress message body: ", err)
		}

		err = lz4Writer.Close()
		if err != nil {
			log.Println("[ERROR] failed to close lz4 writer: ", err)
		}

		err = pipeWriter.Close()
		if err != nil {
			log.Println("[ERROR] failed to close io pipe: ", err)
		}
	}()

	// get compressed bytes from lz4 pipe reader
	buf := new(bytes.Buffer)
	buf.ReadFrom(pipeReader)

	if _, err := w.Next.Write(buf.Bytes()); err != nil {
		return err
	}
	return w.Next.Close()
}

// Write writes bytes to an internal buffer.
func (w *writer) Write(b []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(b)
}
