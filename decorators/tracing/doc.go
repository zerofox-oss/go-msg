// Tracing provides decorators which enable distributed tracing
//
// How it works
//
// This package provides two decorators which can be used to
// propagate tracing information. The topic decorator "tracing.Topic"
// will automatically attach tracing information to any outgoing
// messages. If no parent trace exists, it will create one automatically.
// The second decorator, tracing.Receiver is used to decode tracing information
// into the context.Context object which is passed into the receiver that you
// provide handle messages. Again if to trace is present a trace is started and
// set in the context.
//
// Examples
//
// Using the tracing.Topic:
//
//	func ExampleTopic() {
//		// make a concrete topic eg SNS
//		topic, _ := sns.NewTopic("arn://sns:xxx")
//		// make a tracing topic with the span name "msg.Writer"
//		topic := tracing.TracingTopic(topic, tracing.WithSpanName("msg.Writer"))
//		// use topic as you would without tracing
//	}
//
// Using the tracing.Receiver:
//
//	func ExampleReceiver() {
//      receiver := msg.Receiver(func(ctx context.Context, m *msg.Message) error {
//          // your receiver implementation
//          // ctx will contain tracing information
//          // once decorated
//      })
//      receiver := tracing.Receiver(receiver)
//      // use receiver as you would without tracing
//	}
//
package tracing
