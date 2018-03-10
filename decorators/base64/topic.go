package base64

import (
	"bytes"
	"context"
	"encoding/base64"
	"sync"

	"github.com/zerofox-oss/go-msg"
)

// Encoder wraps a topic with another which base64-encodes a Message.
func Encoder(next msg.Topic) msg.Topic {
	return msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		return &encodeWriter{
			Next: next.NewWriter(ctx),
		}
	})
}

type encodeWriter struct {
	Next msg.MessageWriter

	buf    bytes.Buffer
	closed bool
	mux    sync.Mutex
}

// Attributes returns the attributes associated with the MessageWriter.
func (w *encodeWriter) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}

// Close base64-encodes the contents of the buffer before
// writing them to the next MessageWriter.
func (w *encodeWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed == true {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	attrs := *w.Attributes()
	attrs["Content-Transfer-Encoding"] = []string{"base64"}

	src := w.buf.Bytes()
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(src)))
	base64.StdEncoding.Encode(buf, src)

	if _, err := w.Next.Write(buf); err != nil {
		return err
	}
	return w.Next.Close()
}

// Write writes bytes to an internal buffer.
func (w *encodeWriter) Write(b []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed == true {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(b)
}
