package multiserver

import (
	"context"
	"errors"
	"time"

	msg "github.com/zerofox-oss/go-msg"
	"golang.org/x/sync/errgroup"
)

// MultiServer is a server that can serve messages from multiple underlying servers
// to as single receiver. The server will consume messages from the underlying servers
// in the ratio of the weights provided.
type MultiServer struct {
	servers       []msg.Server
	weights       []float64
	concurrency   int
	queueWaitTime time.Duration
	wfr           *WeightedFairReceiver
}

type ServerWeight struct {
	Server msg.Server
	Weight float64
}

// MultiServerOption is a functional option for the MultiServer.
type MultiServerOption func(*MultiServer)

// WithQueueWaitTime sets the time to wait for a message to arrive.
func WithQueueWaitTime(queueWaitTime time.Duration) MultiServerOption {
	return func(m *MultiServer) {
		m.queueWaitTime = queueWaitTime
	}
}

// NewMultiServer creates a new MultiServer with the given concurrency.
// The server will distribute the messages to underlying receiver from
// the given servers in the ratio of the weights provided.
func NewMultiServer(concurrency int, serverWeights []ServerWeight, opts ...MultiServerOption) (*MultiServer, error) {
	if len(serverWeights) == 0 {
		return nil, errors.New("serverWeights must not be empty")
	}

	if concurrency <= 0 {
		return nil, errors.New("concurrency must be greater than 0")
	}

	servers := make([]msg.Server, 0, len(serverWeights))
	weights := make([]float64, 0, len(serverWeights))

	for _, s := range serverWeights {
		servers = append(servers, s.Server)
		weights = append(weights, s.Weight)
	}

	server := &MultiServer{
		concurrency:   concurrency,
		servers:       servers,
		weights:       weights,
		queueWaitTime: 1 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(server)
	}

	return server, nil
}

// Serve serves messages to the underlying servers.
func (m *MultiServer) Serve(msg msg.Receiver) error {
	m.wfr = NewWeightedFairReceiver(
		m.weights,
		m.concurrency,
		m.queueWaitTime,
		msg,
	)

	g := errgroup.Group{}
	for i, s := range m.servers {
		s := s
		i := i
		g.Go(func() error {
			return s.Serve(m.wfr.WithPriorityReceiver(i))
		})
	}

	return g.Wait()
}

// Shutdown shuts down the server.
func (m *MultiServer) Shutdown(ctx context.Context) error {
	g := errgroup.Group{}
	for _, s := range m.servers {
		s := s
		g.Go(func() error {
			return s.Shutdown(ctx)
		})
	}
	err := g.Wait()
	if err != nil {
		return err
	}

	return m.wfr.Close(ctx)
}
