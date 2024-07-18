package multiserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/backends/mem"
	"github.com/zerofox-oss/go-msg/x/multiserver"
	"pgregory.net/rapid"
)

func sendMessages(ctx context.Context, inputChan chan *msg.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case inputChan <- &msg.Message{
			Body:       bytes.NewBuffer([]byte("hello world")),
			Attributes: msg.Attributes{},
		}:
		}
	}
}

func TestMultiServer(t *testing.T) {
	t.Parallel()

	for i := 0; i <= 10; i++ {
		t.Run(fmt.Sprintf("TestMultiServer_%d", i), func(t *testing.T) {
			t.Parallel()

			rapid.Check(t, func(t *rapid.T) {
				numServers := rapid.IntRange(1, 10).Draw(t, "numServers")

				serverConcurrency := 10
				inputChanBuffer := 100

				counts := make([]atomic.Int32, numServers)
				inputChans := make([]chan *msg.Message, numServers)
				serverWeights := make([]multiserver.ServerWeight, numServers)

				for i := 0; i < numServers; i++ {
					inputChan := make(chan *msg.Message, inputChanBuffer)
					server := mem.NewServer(inputChan, serverConcurrency)
					weight := rapid.IntRange(1, 10).Draw(t, "weight")
					serverWeights[i] = multiserver.ServerWeight{
						Server: server,
						Weight: float64(weight),
					}
					inputChans[i] = inputChan
				}

				mserver, err := multiserver.NewMultiServer(serverConcurrency, serverWeights)
				assert.NoError(t, err)

				go func() {
					mserver.Serve(msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
						msgPriority := m.Attributes.Get(multiserver.MultiServerMsgPriority)
						p, err := strconv.Atoi(msgPriority)
						if err != nil {
							assert.Fail(t, "failed to parse priority")
						}
						counts[p].Add(1)
						time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
						return nil
					}))
				}()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				for i := 0; i < numServers; i++ {
					go sendMessages(ctx, inputChans[i])
				}

				<-ctx.Done()
				mserver.Shutdown(context.Background())

				totalWeight := 0
				totalCounts := 0
				for i := 0; i < numServers; i++ {
					totalWeight += int(serverWeights[i].Weight)
					totalCounts += int(counts[i].Load())
				}

				delta := float64(totalCounts) * float64(0.1)

				t.Logf("Total weight: %d\n", totalWeight)
				t.Logf("Total counts: %d\n", totalCounts)
				t.Logf("Total counts delta: %f\n", delta)

				for i := 0; i < numServers; i++ {
					weight := int(serverWeights[i].Weight)
					expectedCount := (weight * totalCounts) / totalWeight
					serverCount := int(counts[i].Load())
					t.Logf("Server %d: weight %d, expected count %d, actual count %d\n", i, weight, expectedCount, serverCount)
					assert.InDeltaf(
						t, expectedCount, serverCount, delta,
						"Server %d: weight %d, expected count %d, actual count %d\n", i, weight, expectedCount, serverCount)
				}
			})
		})
	}
}
