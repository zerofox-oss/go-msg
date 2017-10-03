package mem_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/mem"
)

// ConcurrentReceiver writes to an channel upon consumption of a Message.
// It is safe to utilize by concurrent goroutines.
type ConcurrentReceiver struct {
	t *testing.T
	C chan struct{}
}

func (r *ConcurrentReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		r.C <- struct{}{}
	}
	return nil
}

// PanicReceiver panics upon consumption of a message.
// It is safe to utilize by concurrent goroutines.
type PanicReceiver struct {
	t *testing.T
}

func (r *PanicReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		panic("AHHH")
	}
}

// RetryReceiver returns an error upon consumption of a Message. Once it
// has been called a certain number of times, it writes to an channel and
// returns nil.
//
// It it safe to utilize by concurrent goroutines.
type RetryReceiver struct {
	t *testing.T
	C chan struct{}

	allowedRetries int
	calls          int
}

func (r *RetryReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		r.t.Fatalf("[ERROR] Receiver could not finish executing")
	default:
	}

	if r.calls == r.allowedRetries {
		r.C <- struct{}{}
		return nil
	}
	r.calls++

	return errors.New("could not complete transaction")
}

// TestServer_Serve asserts that the Server can process all messages that
// have been sent to its input buffer using a single goroutine
func TestServer_Serve(t *testing.T) {
	messages := []*msg.Message{
		{
			Attributes: msg.Attributes{},
			Body:       bytes.NewBufferString("message #1: hello world!"),
		},
		{
			Attributes: msg.Attributes{},
			Body:       bytes.NewBufferString("message #2: foo bar"),
		},
		{
			Attributes: msg.Attributes{},
			Body:       bytes.NewBufferString("message #3: gophercon9000"),
		},
	}

	srv := mem.NewServer(make(chan *msg.Message, len(messages)), 1)

	for _, m := range messages {
		srv.C <- m
	}
	defer close(srv.C)

	outputChannel := make(chan struct{})
	go func() {
		srv.Serve(&ConcurrentReceiver{
			t: t,
			C: outputChannel,
		})
	}()

	// block until all requests have been made
	calls := 0
	for range outputChannel {
		calls++
		if calls == len(messages) {
			close(outputChannel)
		}
	}
}

// TestServer_Serve asserts that the Server can process all messages that
// have been sent to its input buffer using lots of concurrent goroutines
func TestServer_ServeConcurrency(t *testing.T) {
	var messages [10000]*msg.Message

	for i := 0; i < len(messages); i++ {
		messages[i] = &msg.Message{
			Attributes: msg.Attributes{},
			Body:       bytes.NewBufferString(fmt.Sprintf("this is a test message #%d", i)),
		}
	}

	srv := mem.NewServer(make(chan *msg.Message, len(messages)), 10)

	for _, m := range messages {
		srv.C <- m
	}
	defer close(srv.C)

	outputChannel := make(chan struct{})
	go func() {
		srv.Serve(&ConcurrentReceiver{
			t: t,
			C: outputChannel,
		})
	}()

	// block until all requests have been made
	calls := 0
	for range outputChannel {
		calls++
		if calls == len(messages) {
			close(outputChannel)
		}
	}
}

// TestServer_ServeCanRetryMessages asserts that the Server can handle
// errors returned by the Receiver and "retry" the message that caused
// the error
func TestServer_ServeCanRetryMessages(t *testing.T) {
	messages := []*msg.Message{
		{
			Attributes: msg.Attributes{},
			Body:       bytes.NewBufferString("message #1: hello world!"),
		},
	}

	srv := mem.NewServer(make(chan *msg.Message, len(messages)), 1)

	for _, m := range messages {
		srv.C <- m
	}
	defer close(srv.C)

	outputChannel := make(chan struct{})
	go func() {
		srv.Serve(&RetryReceiver{
			t: t,
			C: outputChannel,

			calls:          0,
			allowedRetries: 10,
		})
	}()

	// after 10th retry receiver will write to channel
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatalf("context timed out")
	case <-outputChannel:
		close(outputChannel)
	}
}

// TestServer_ShutdownContextTimeoutExceeded tests that the Server can
// use a Context to shut down before all of it's goroutines have finished.
func TestServer_ShutdownContextTimeoutExceeded(t *testing.T) {
	done := make(chan struct{})

	message := &msg.Message{
		Attributes: msg.Attributes{},
		Body:       bytes.NewBufferString("hello world!"),
	}

	srv := mem.NewServer(make(chan *msg.Message, 1), 1)
	defer close(srv.C)

	receiver := msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		defer close(done)

		// set a fast timeout so srv doesn't have enough time to finish
		sctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		if err := srv.Shutdown(sctx); err == msg.ErrServerClosed {
			t.Fatal("expected context timeout")
		}
		return nil
	})
	srv.C <- message

	if err := srv.Serve(receiver); err != msg.ErrServerClosed {
		t.Fatalf("expected %v, got %v", msg.ErrServerClosed, err)
	}

	<-done
}
