package base64

import (
	"context"
	"encoding/base64"

	"github.com/zerofox-oss/go-msg"
)

// Decoder wraps a msg.Receiver with base64 decoding functionality.
// It only attempts to decode the Message.Body if Content-Transfer-Encoding
// is set to base64.
func Decoder(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if isBase64Encoded(m) {
				m.Body = base64.NewDecoder(base64.StdEncoding, m.Body)
			}

			return next.Receive(ctx, m)
		}
	})
}

// isBase64Encoded returns true if Content-Transfer-Encoding is set to
// "base64" in the passed Message's Attributes.
//
// Note: MIMEHeader.Get() is used to fetch this value. In the case of a list of
// values, .Get() returns the 0th value.
func isBase64Encoded(m *msg.Message) bool {
	if m.Attributes.Get("Content-Transfer-Encoding") == "base64" {
		return true
	}

	return false
}
