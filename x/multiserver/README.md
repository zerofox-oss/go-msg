# Multiserver

The `multiserver` package provides a `MultiServer` that can serve messages from multiple underlying servers to a single receiver. The server will consume messages from the underlying servers in the ratio of the weights provided.

### Example

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	"github.com/zerofox-oss/go-msg/x/multiserver"
)

func main() {
	// Create memory servers
	server1 := mem.NewServer(make(chan *msg.Message, 100), 10)
	server2 := mem.NewServer(make(chan *msg.Message, 100), 10)

	// Define server weights
	serverWeights := []multiserver.ServerWeight{
		{Server: server1, Weight: 1.0},
		{Server: server2, Weight: 2.0},
	}

	// Create MultiServer
	mserver, err := multiserver.NewMultiServer(10, serverWeights)
	if err != nil {
		fmt.Println("Error creating MultiServer:", err)
		return
	}

	// Start serving messages
	go func() {
		mserver.Serve(msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			fmt.Println("Received message:", m)
			return nil
		}))
	}()

	// Simulate sending messages
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case server1.C <- &msg.Message{Body: bytes.NewBuffer([]byte("message from server1"))}:
			case server2.C <- &msg.Message{Body: bytes.NewBuffer([]byte("message from server2"))}:
			}
		}
	}()

	<-ctx.Done()
	mserver.Shutdown(context.Background())
}
```
