package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zerofox-oss/go-msg"
)

type Server struct {
	Stream      string
	Group       string
	Consumer    string // Consumer should be unique, so if we horizontally scale a component each alloc gets its own ID
	Concurrency int

	conn *redis.Client

	inFlightQueue chan struct{}

	// context used to shutdown processing of in-flight messages
	receiverCtx        context.Context
	receiverCancelFunc context.CancelFunc

	// context used to shutdown the server
	serverCtx        context.Context
	serverCancelFunc context.CancelFunc
}

func (s *Server) Serve(r msg.Receiver) error {
	// XREADGROUP specifies a STREAMS <id> parameter which controls how messages are read.
	// Passing "0" means it will read pending messages in the consumer group which were not XACK'd.
	// This can happen if the server crashes.
	// We will read in this mode until all pending messages are gone.
	// Then we can switch to ID = ">", which means read new messages.
	var streamID = ">"

	for {
		select {
		case <-s.serverCtx.Done():
			close(s.inFlightQueue)

			return msg.ErrServerClosed
		default:
			ctx, cancel := context.WithTimeout(s.receiverCtx, 30*time.Second)
			defer cancel()

			resp := s.conn.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    s.Group,
				Consumer: s.Consumer,
				Streams:  []string{s.Stream, streamID},
				Count:    int64(10),
			})
			if err := resp.Err(); err != nil {
				return fmt.Errorf("failed to read messages with: %w", err)
			}

			// otherwise, we have messages to process
			for _, m := range resp.Val()[0].Messages {
				s.inFlightQueue <- struct{}{}

				go func(m redis.XMessage) {
					defer func() {
						<-s.inFlightQueue
					}()

					if err := s.process(m, r); err != nil {
						log.Println(err)
					}
				}(m)
			}
		}
	}
}

func (s *Server) process(m redis.XMessage, r msg.Receiver) error {
	message, err := toMessage(m.Values)
	if err != nil {
		return fmt.Errorf("could not marshal redis.XMessage: %w", err)
	}

	if err := r.Receive(s.receiverCtx, &message); err != nil {
		throttleErr, ok := err.(msg.ErrServerThrottled)
		if ok {
			time.Sleep(throttleErr.Duration)
			return nil
		}

		return fmt.Errorf("receiver error: %w", err)
	}

	// ACK the message to remove it
	ctx, cancel := context.WithTimeout(s.receiverCtx, 10*time.Second)
	defer cancel()

	resp := s.conn.XAck(ctx, s.Stream, s.Group, m.ID)
	if err := resp.Err(); err != nil {
		return fmt.Errorf("could not XACK message: %w", err)
	}

	return nil
}

const shutdownPollInterval = 500 * time.Millisecond

// Shutdown stops the receipt of new messages and waits for routines
// to complete or the passed in ctx to be canceled. msg.ErrServerClosed
// will be returned upon a clean shutdown. Otherwise, the passed ctx's
// Error will be returned.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set")
	}

	s.serverCancelFunc()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.receiverCancelFunc()

			return ctx.Err()
		case <-ticker.C:
			if len(s.inFlightQueue) == 0 {
				return msg.ErrServerClosed
			}
		}
	}
}

// Option is the signature that modifies a `Server` to set some configuration
type Option func(*Server) error

func NewServer(client *redis.Client, stream, group, consumer string, opts ...Option) (*Server, error) {
	defaultConcurrency := 10

	serverCtx, serverCancelFunc := context.WithCancel(context.Background())
	receiverCtx, receiverCancelFunc := context.WithCancel(context.Background())

	srv := &Server{
		Stream:      stream,
		Group:       group,
		Consumer:    consumer,
		Concurrency: defaultConcurrency,

		conn:          client,
		inFlightQueue: make(chan struct{}, defaultConcurrency),

		receiverCtx:        receiverCtx,
		receiverCancelFunc: receiverCancelFunc,
		serverCtx:          serverCtx,
		serverCancelFunc:   serverCancelFunc,
	}

	return srv, nil
}

// Topic publishes Messages to a Redis Stream.
type Topic struct {
	Stream string
	Conn   *redis.Client
}

// NewWriter returns a MessageWriter.
// The MessageWriter may be used to write messages to a Reis Stream.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		ctx:        ctx,
		attributes: make(map[string][]string),
		buf:        &bytes.Buffer{},

		stream: t.Stream,
		conn:   t.Conn,
	}
}

// MessageWriter is used to publish a single Message to a Redis Stream.
// Once all of the data has been written and closed, it may not be used again.
type MessageWriter struct {
	msg.MessageWriter

	stream string
	conn   *redis.Client

	ctx        context.Context
	attributes msg.Attributes
	buf        *bytes.Buffer // internal buffer
	closed     bool
	mux        sync.Mutex
}

// Attributes returns the attributes of the MessageWriter.
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

// Close publishes a Message.
// If the MessageWriter is already closed it will return an error.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	if w.buf.Len() > 0 {
		message, err := toMap(msg.Message{
			Attributes: *w.Attributes(),
			Body:       w.buf,
		})
		if err != nil {
			return err
		}

		res := w.conn.XAdd(w.ctx, &redis.XAddArgs{
			Stream: w.stream,
			ID:     "*",
			Values: message,
		})

		return res.Err()
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

func toMap(m msg.Message) (map[string]interface{}, error) {
	return map[string]interface{}{
		"Attributes": nil,
		"Body":       fmt.Sprint(m.Body),
	}, nil
}

// converts map[string]interface{} to a Message
func toMessage(m map[string]interface{}) (msg.Message, error) {
	return msg.Message{
		Attributes: nil,
		Body:       bytes.NewBufferString(fmt.Sprint(m["Body"])),
	}, nil
}
