package mem

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/zerofox-oss/go-msg"
)

// Topic publishes Messages to a channel.
type Topic struct {
	C chan *msg.Message
}

// Ensure that Topic implements msg.Topic
var _ msg.Topic = &Topic{}

// NewWriter returns a MessageWriter.
// The MessageWriter may be used to write messages to a channel.
func (t *Topic) NewWriter(context.Context) msg.MessageWriter {
	return &MessageWriter{
		c: t.C,

		attributes: make(map[string][]string),
		buf:        &bytes.Buffer{},
	}
}

// MessageWriter is used to publish a single Message to a channel.
// Once all of the data has been written and closed, it may not be used again.
type MessageWriter struct {
	msg.MessageWriter

	c chan *msg.Message

	attributes msg.Attributes
	buf        *bytes.Buffer // internal buffer
	closed     bool
	mux        sync.Mutex
}

// Attributes returns the attributes of the MessageWriter.
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

func (w *MessageWriter) SetDelay(_ time.Duration) {
	return
}

// Close publishes a Message to a channel.
// If the MessageWriter is already closed it will return an error.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	if w.buf.Len() > 0 {
		msg := &msg.Message{
			Attributes: w.attributes,
			Body:       w.buf,
		}
		w.c <- msg
	}

	return nil
}

// Write writes bytes to an internal buffer.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}
