package wfr

import (
	"context"
	"time"

	pq "github.com/JimWen/gods-generic/queues/priorityqueue"
	"github.com/JimWen/gods-generic/utils"
	"github.com/zerofox-oss/go-msg"
)

type weightedMessage struct {
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

	receiveChan chan *weightedMessage
	queue       *pq.Queue[*weightedMessage]
	startTime   int
	lastVFinish []float64
}

func (w *WeightedFairReceiver) WithPriorityReceiver(priority int) msg.Receiver {
	if priority < 0 || priority >= len(w.weights) {
		panic("invalid priority")
	}
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		return w.receiveWithPriority(ctx, m, priority)
	})
}

func (r *WeightedFairReceiver) receiveWithPriority(ctx context.Context, m *msg.Message, priority int) error {
	wm := &weightedMessage{
		msg:      m,
		priority: priority,
		doneChan: make(chan error, 1),
		context:  ctx,
	}
	r.receiveChan <- wm
	return <-wm.doneChan
}

func (r *WeightedFairReceiver) dispatch(ctx context.Context) {
	maxConcurrentReceives := make(chan struct{}, r.maxConcurrent)
	ticker := time.NewTicker(r.queueWaitTime)

    // New messages to be processed are enqueued from the receiveChan
    // with their virtual finish time calculated based on the priority.
    // Every tick we select the message with the smallest virtual finish
    // time and process it.

	for {
		select {
        case <-ctx.Done():
            return
		case wm := <-r.receiveChan:
			vStart := max(float64(r.startTime), r.lastVFinish[wm.priority])
			weight := float64(1.0 / r.weights[wm.priority])
			vFinish := vStart + weight
			wm.vFinish = vFinish
			r.lastVFinish[wm.priority] = vFinish
			r.queue.Enqueue(wm)
		case <-ticker.C:
			mw, ok := r.queue.Dequeue()
			if !ok {
				continue
			}

			maxConcurrentReceives <- struct{}{}
			go func() {
				defer func() {
					<-maxConcurrentReceives
				}()
				mw.doneChan <- r.receiver.Receive(mw.context, mw.msg)
			}()
		}
	}
}

type WeightedFairReceiverOption func(*WeightedFairReceiver)

// WithQueueWaitTime sets the time to wait for a message to arrive.
// The longer you wait, the better the fairness.
func WithQueueWaitTime(d time.Duration) WeightedFairReceiverOption {
	return func(r *WeightedFairReceiver) {
		r.queueWaitTime = d
	}
}

// WithMaxConcurrent sets the maximum number of concurrent messages that can be processed
// by the receiver. If the maximum number of concurrent messages is reached, the receiver
// will block until a message is processed.
func WithMaxConcurrent(max int) WeightedFairReceiverOption {
	return func(r *WeightedFairReceiver) {
		r.maxConcurrent = max
	}
}

// NewWeightedFairReceiver creates a new WeightedFairReceiver with the given receiver and weights.
// The receiver will process messages based on their priority and the weights provided.
// Example:
//
//	 // The receiver will ensure that messages with priority 0 are processed 1/6 of the time,
//	 // messages with priority 1 are processed 1/3 of the time and messages with priority 2 are
//	 // processed 1/2 of the time.
//	 r := NewWeightedFairReceiver(ctx, receiver, []float64{1, 2, 3})
//	 r0 = r.WithPriorityReceiver(0) // will process messages with priority 0
//	 r1 = r.WithPriorityReceiver(1) // will process messages with priority 1
//	 r2 = r.WithPriorityReceiver(2) // will process messages with priority 2
//		srv0, err := sqs.NewServer(c.String("sqs-queue-url-0"), 50, 100)
//	 if err != nil {
//	     return fmt.Errorf("failed to establish SQS connection: %w", err)
//	 }
//	 srv1, err := sqs.NewServer(c.String("sqs-queue-url-1"), 50, 100)
//	 if err != nil {
//	     return fmt.Errorf("failed to establish SQS connection: %w", err)
//	 }
//	 srv2, err := sqs.NewServer(c.String("sqs-queue-url-2"), 50, 100)
//	 if err != nil {
//	     return fmt.Errorf("failed to establish SQS connection: %w", err)
//	 }
//	 errGroup, _ := errgroup.WithContext(context.Background())
//	 errGroup.Go(func() error {
//	     return srv0.Serve(lowPriorityReceiver)
//	 })
//	 errGroup.Go(func() error {
//	     return srv1.Serve(mediumPriorityReceiver)
//	 })
//	 errGroup.Go(func() error {
//	     return srv2.Serve(highPriorityReceiver)
//	 })
//	 errGroup.Wait()
func NewWeightedFairReceiver(ctx context.Context, receiver msg.Receiver, weights []float64, opts ...WeightedFairReceiverOption) *WeightedFairReceiver {

	priorityQueue := pq.NewWith(func(a, b *weightedMessage) int {
		return utils.NumberComparator(a.vFinish, b.vFinish)
	})

	r := &WeightedFairReceiver{
		receiver:      receiver,
		weights:       weights,
		queueWaitTime: 50 * time.Millisecond,
		maxConcurrent: 10,
		receiveChan:   make(chan *weightedMessage),
		queue:         priorityQueue,
		startTime:     int(time.Now().Unix()),
		lastVFinish:   make([]float64, len(weights)),
	}

	for _, opt := range opts {
		opt(r)
	}

	go r.dispatch(ctx)
	return r
}
