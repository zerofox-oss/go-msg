package multiserver

import (
	"context"
	"fmt"
	"math"
	"time"

	pq "github.com/JimWen/gods-generic/queues/priorityqueue"
	"github.com/JimWen/gods-generic/utils"
	"github.com/asecurityteam/rolling"
	msg "github.com/zerofox-oss/go-msg"
)

// MultiServerMsgPriority is the key used to store the priority of a message in the message attributes.
const MultiServerMsgPriority = "x-multiserver-priority"

type weightedMessage struct {
	msg      *msg.Message
	context  context.Context
	vFinish  float64
	priority int
	doneChan chan error
}

// WeightedFairReceiver implements a "fair" receiver that processes messages based on their priority.
// See https://en.wikipedia.org/wiki/Fair_queuing for more information, about fairness.
// The concrete implementation is based on the Weighted Fair Queuing algorithm.
// At a high level the algorithm works as follows:
//  1. Each message is assigned a virtual finish time. The virtual finish time is defined as the
//     time of the last message processed for that priority plus a weighting factor. The higher the
//     the weight the smaller the virtual finish time is for that message.
//  2. The message is then enqueued in a priority queue based on the virtual finish time.
//  3. Every "tick" we select the message with the smallest virtual finish time and process it.
type WeightedFairReceiver struct {
	// underlying receiver
	receiver msg.Receiver

	// time to wait for a message to arrive
	// the longer you wait, the better the fairness
	// the shorter the better the latency.
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

	receiveChan chan *weightedMessage
	queue       *pq.Queue[*weightedMessage]
	startTime   int
	lastVFinish []float64

	timePolicy *rolling.TimePolicy
	closeChan  chan chan error
}

// NewWeightedFairReceiver creates a new WeightedFairReceiver.
// The receiver will process messages based on their priority level.
func NewWeightedFairReceiver(
	weights []float64,
	maxConcurrent int,
	queueWaitTime time.Duration,
	receiver msg.Receiver,
) *WeightedFairReceiver {
	priorityQueue := pq.NewWith(func(a, b *weightedMessage) int {
		return utils.NumberComparator(a.vFinish, b.vFinish)
	})

	wfr := &WeightedFairReceiver{
		weights:              weights,
		lastVFinish:          make([]float64, len(weights)),
		receiver:             receiver,
		queue:                priorityQueue,
		startTime:            1,
		queueWaitTime:        queueWaitTime,
		maxConcurrent:        maxConcurrent,
		receiveChan:          make(chan *weightedMessage),
		timePolicy:           rolling.NewTimePolicy(rolling.NewWindow(10000), 1*time.Millisecond),
		initialEstimatedCost: 100.0,
		closeChan:            make(chan chan error),
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
	wm := &weightedMessage{
		msg:      m,
		priority: priority,
		doneChan: make(chan error, 1),
		context:  ctx,
	}
	w.receiveChan <- wm
	return <-wm.doneChan
}

// Close closes the receiver.
func (w *WeightedFairReceiver) Close(ctx context.Context) error {
	doneChan := make(chan error)
	w.closeChan <- doneChan
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-doneChan:
		return err
	}
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
				go func(wm weightedMessage) {
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
		case doneChan := <-w.closeChan:
			doneChan <- nil
			// TODO: process remaining messages
			// that are queued in order to cleanly shutdown
			return
		case wm := <-w.receiveChan:
			vStart := math.Max(float64(w.startTime), w.lastVFinish[wm.priority])
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
