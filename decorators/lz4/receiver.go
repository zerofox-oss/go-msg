package lz4

import (
	"context"

	"github.com/pierrec/lz4/v4"
	"github.com/zerofox-oss/go-msg"
)

// Decoder wraps a msg.Receiver with lz4 decoding functionality.
// It only attempts to decode the Message.Body if Content-Encoding
// is set to lz4. This should be used in conjuction with the base64
// decode decorator, when the message queue doesn't support binary.
// In this case the base64 decorator should be the outermost decorator
// in order to run first.
func Decoder(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		if isLz4Compressed(m) {
			m.Body = lz4.NewReader(m.Body)
		}
		return next.Receive(ctx, m)
	})
}

// isLz4Compressedreturns true if Content-Encoding is set to
// "lz4" in the passed Message's Attributes.
//
// Note: MIMEHeader.Get() is used to fetch this value. In the case of a list of
// values, .Get() returns the 0th value.
func isLz4Compressed(m *msg.Message) bool {
	return m.Attributes.Get("Content-Encoding") == "lz4"
}
