package multiserver

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/asecurityteam/rolling"
	"golang.org/x/sync/errgroup"

	_ "net/http/pprof"

	pq "github.com/JimWen/gods-generic/queues/priorityqueue"
	"github.com/JimWen/gods-generic/utils"
	msg "github.com/zerofox-oss/go-msg"
)

const MultiServerMsgPriority = "x-multiserver-priority"

type WeightedMessage struct {
	msg      *msg.Message
	context  context.Context
	vFinish  float64
	priority int
	doneChan chan error
}

// WeightedFairReceiver implements a "fair" receiver that processes messages based on their priority.
// See https://en.wikipedia.org/wiki/Fair_queuing for more information, about fairness.
// The concrete implmentation is based on the Weighted Fair Queuing algorithm.
// At a high level the algorithm works as follows:
//  1. Each message is asigned a virtual finish time. The virtual finish time is defined as the
//     time of the last message processed for that priority plus a weigting factor. The higher the
//     the weight the smaller the virtual finish time is for that message.
//  2. The message is then enqueued in a priority queue based on the virtual finish time.
//  3. Every "tick" we select the the message with the smallest virtual finish time and process it.
type WeightedFairReceiver struct {

	// underlying receiver
	receiver msg.Receiver

	// time to wait for a message to arrive
	// the longer you wait, the better the fairness
	// the shorter the better the latency
	// a good value should be around the mean processing
	// time for your receiver. If in doubt 50ms may be good.
	queueWaitTime time.Duration

	// weights for each priority level
	// the higher the weight, the more messages
	// will be processed for that priority
	weights []float64

	// max number of concurrent messages that can
	// be processed at the same time by the receiver
	maxConcurrent int
    
    // a rough estimate time to process a message
    // this should be in milliseconds.
    initialEstimatedCost float64

	receiveChan chan *WeightedMessage
	queue       *pq.Queue[*WeightedMessage]
	startTime   int
	lastVFinish []float64

	timePolicy *rolling.TimePolicy
}

func NewWeightedFairReceiver(
	weights []float64,
	maxConcurrent int,
	queueWaitTime time.Duration,
	receiver msg.Receiver,
) *WeightedFairReceiver {

	priorityQueue := pq.NewWith(func(a, b *WeightedMessage) int {
		return utils.NumberComparator(a.vFinish, b.vFinish)
	})

	wfr := &WeightedFairReceiver{
		weights:       weights,
		lastVFinish:   make([]float64, len(weights)),
		receiver:      receiver,
		queue:         priorityQueue,
		startTime:     1,
		queueWaitTime: queueWaitTime,
		maxConcurrent: maxConcurrent,
		receiveChan:   make(chan *WeightedMessage),
		timePolicy:  rolling.NewTimePolicy(rolling.NewWindow(10000), 1*time.Millisecond),
        initialEstimatedCost: 100.0,
	}

	go wfr.dispatch()
	return wfr
}

// WithPriorityReceiver returns a new msg.Receiver that should be used to receive messages 
// at a specific priority level.
func (w *WeightedFairReceiver) WithPriorityReceiver(priority int) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		return w.Receive(ctx, m, priority)
	})
}

// Receive receives a message with a specific priority level.
func (w *WeightedFairReceiver) Receive(ctx context.Context, m *msg.Message, priority int) error {
	wm := &WeightedMessage{
		msg:      m,
		priority: priority,
		doneChan: make(chan error, 1),
		context:  ctx,
	}
	w.receiveChan <- wm
	return <-wm.doneChan
}

func (w *WeightedFairReceiver) estimateCost() float64 {
	return w.timePolicy.Reduce(rolling.Avg)
}

func (w *WeightedFairReceiver) dispatch() {
	maxConcurrentReceives := make(chan struct{}, w.maxConcurrent)
	timer := time.NewTicker(w.queueWaitTime)

	doReceive := func() {
		select {
		case maxConcurrentReceives <- struct{}{}:
			if mw, ok := w.queue.Dequeue(); ok {
				go func(wm WeightedMessage) {
					defer func() {
						<-maxConcurrentReceives
					}()
					st := time.Now()
					wm.msg.Attributes.Set(MultiServerMsgPriority, fmt.Sprint(wm.priority))
					result := w.receiver.Receive(wm.context, wm.msg)
					w.timePolicy.Append(float64(time.Since(st).Milliseconds()))
					wm.doneChan <- result
				}(*mw)
			} else {
				<-maxConcurrentReceives
			}
		default:
		}
	}

	for {
		select {
		case wm := <-w.receiveChan:
			vStart := max(float64(w.startTime), w.lastVFinish[wm.priority])
			estimatedCost := w.estimateCost()
			if math.IsNaN(estimatedCost) {
				estimatedCost = w.initialEstimatedCost
			}
			estimatedCost = math.Round(estimatedCost)
			weight2 := float64(estimatedCost / w.weights[wm.priority])
			vFinish := vStart + weight2
			wm.vFinish = vFinish
			w.lastVFinish[wm.priority] = vFinish
			w.queue.Enqueue(wm)
		case <-timer.C:
			doReceive()
		}
	}
}

type MultiServer struct {
	servers       []msg.Server
	weights       []float64
	concurrency   int
	queueWaitTime time.Duration
}

type serverWeight struct {
	server msg.Server
	weight float64
}

type MultiServerOption func(*MultiServer)

func WithServer(server msg.Server, weight float64) MultiServerOption {
	return func(m *MultiServer) {
		m.servers = append(m.servers, server)
		m.weights = append(m.weights, weight)
	}
}

func WithQueueWaitTime(queueWaitTime time.Duration) MultiServerOption {
	return func(m *MultiServer) {
		m.queueWaitTime = queueWaitTime
	}
}

func NewMultiServer(concurrency int, opts ...MultiServerOption) (*MultiServer, error) {
	server := &MultiServer{
		concurrency:   concurrency,
		servers:       make([]msg.Server, 0),
		weights:       make([]float64, 0),
		queueWaitTime: 1 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(server)
	}

	fmt.Println("server weights", server.weights)

	return server, nil
}

func (m *MultiServer) Serve(msg msg.Receiver) error {
	wfr := NewWeightedFairReceiver(
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
			return s.Serve(wfr.WithPriorityReceiver(i))
		})
	}

	return g.Wait()
}

func (m *MultiServer) Shutdown(ctx context.Context) error {
	g := errgroup.Group{}
	for _, s := range m.servers {
		s := s
		g.Go(func() error {
			return s.Shutdown(ctx)
		})
	}
	return g.Wait()
}
