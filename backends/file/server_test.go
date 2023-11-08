package file_test

import (
	"context"
	"log"
	"testing"

	msg "github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/file"
)

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

func TestServe(t *testing.T) {

	log.Println("running testserve for file")

	srv := file.NewServer("fixtures", false, 1)

	outputChannel := make(chan struct{})
	go func() {
		srv.Serve(&ConcurrentReceiver{
			t: t,
			C: outputChannel,
		})
	}()

	calls := 0
	for range outputChannel {
		calls++
		if calls == 3 {
			close(outputChannel)
		}
	}

}
