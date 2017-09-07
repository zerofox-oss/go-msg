package msg

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/textproto"
)

// Attributes represent the key-value metadata for a Message.
type Attributes map[string][]string

// from https://golang.org/src/net/http/header.go#L62
func (a Attributes) clone() Attributes {
	a2 := make(Attributes, len(a))
	for k, vv := range a {
		vv2 := make([]string, len(vv))
		copy(vv2, vv)
		a2[k] = vv2
	}
	return a2
}

// Get returns the first value associated with the given key.
// It is case insensitive; CanonicalMIME is used to cannonicalize the provided
// key. If there are no values associated with the key, Get returns "".
// To access multiple values of a key, or to use non-canonical keys,
// access the map directly.
func (a Attributes) Get(key string) string {
	return textproto.MIMEHeader(a).Get(key)
}

// Set sets the header entries associated with key the single element
// element value. It replaces any existing values associated with key.
//
// Note: MIMEHeader automatically capitalizes the first letter of the key.
func (a Attributes) Set(key, value string) {
	textproto.MIMEHeader(a).Set(key, value)
}

// A Message represents a discrete message in a messaging system.
type Message struct {
	Attributes Attributes
	Body       io.Reader
}

// WithBody creates a new Message with the given io.Reader as a Body
// containing the parent's Attributes.
// 	p := &Message{
//		Attributes: Attributes{},
//		Body: strings.NewReader("hello world"),
//	}
//	p.Attributes.Set("hello", "world")
//	m := WithBody(p, strings.NewReader("world hello")
func WithBody(parent *Message, r io.Reader) *Message {
	return &Message{
		Attributes: parent.Attributes.clone(),
		Body:       r,
	}
}

// DumpBody returns the contents of m.Body
// while resetting m.Body
// allowing it to be read from later.
func DumpBody(m *Message) ([]byte, error) {
	b := m.Body
	// inspired by https://golang.org/src/net/http/httputil/dump.go#L26
	var buf bytes.Buffer

	if _, err := buf.ReadFrom(b); err != nil {
		return nil, err
	}
	m.Body = &buf

	return buf.Bytes(), nil
}

// CloneBody returns a reader
// with the same contents and m.Body.
// m.Body is reset allowing it to be read from later.
func CloneBody(m *Message) (io.Reader, error) {
	b, err := DumpBody(m)
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(b), nil
}

// A Receiver processes a Message.
//
// Receive should process the message and then return. Returning signals that
// the message has been processed. It is not valid to read from the Message.Body
// after or concurrently with the completion of the Receive call.
//
// If Receive returns an error, the server (the caller of Receive) assumes the
// message has not been processed and, depending on the underlying pub/sub
// system, the message should be put back on the message queue.
type Receiver interface {
	Receive(context.Context, *Message) error
}

// The ReceiverFunc is an adapter to allow the use of ordinary functions
// as a Receiver. ReceiverFunc(f) is a Receiver that calls f.
type ReceiverFunc func(context.Context, *Message) error

// Receive calls f(ctx,m)
func (f ReceiverFunc) Receive(ctx context.Context, m *Message) error {
	return f(ctx, m)
}

// ErrServerClosed represents a completed Shutdown
var ErrServerClosed = errors.New("msg: server closed")

// A Server serves messages to a receiver.
type Server interface {
	// Serve is a blocking function that gets data from an input stream,
	// creates a message, and calls Receive() on the provided receiver
	// with the Message and a Context derived from context.Background().
	// For example:
	//
	// 		parentctx = context.WithCancel(context.Background())
	// 		err := r.Receive(parentctx, m)
	//
	// Serve will return ErrServerClosed after Shutdown completes. Additional
	// error types should be considered to represent error conditions unique
	// to the implemetnation of a specific technology.
	//
	// Serve() should continue to listen until Shutdown is called on
	// the Server.
	Serve(Receiver) error

	// Shutdown gracefully shuts down the Server by letting any messages in
	// flight finish processing.  If the provided context cancels before
	// shutdown is complete, the Context's error is returned.
	Shutdown(context.Context) error
}

// ErrClosedMessageWriter is the error used for write or close operations on a closed MessageWriter.
var ErrClosedMessageWriter = errors.New("msg: MessageWriter closed")

// A MessageWriter interface is used to write a message to
// an underlying data stream.
type MessageWriter interface {
	io.Writer
	// Close should be called to signify the completion of a Write. Attributes
	// that represent a transform applied to a message should also be written
	// at this time.
	//
	// Close should forward a message to another MessageWriter or persist
	// to the messaging system.
	//
	// Once Close has been called, all subsequent Write and Close calls will result
	// in an ErrClosedMessageWriter error.
	io.Closer
	Attributes() *Attributes
}

// Topic is a generic interface where messages are sent in a messaging system.
//
// Multiple goroutines may invoke method on a Topic simultaneously.
type Topic interface {
	// NewWriter returns a new MessageWriter
	NewWriter() MessageWriter
}
