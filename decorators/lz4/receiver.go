package lz4

import (
	"context"

	lz4algo "github.com/pierrec/lz4"
	"github.com/zerofox-oss/go-msg"
)

// Decompressor wraps a msg.Receiver with decompression functionality.
// It only attempts to decompress the Message.Body if Content-Encoding
// is set to "lz4".
//
// Note: lz4 is a non-standard coding for this header;
// i.e. it is not defined in RFC 2616:
// https://datatracker.ietf.org/doc/html/rfc2616#section-3.5).
func Decompressor(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if isLZ4Compressed(m) {
				m.Body = lz4algo.NewReader(m.Body)
			}

			return next.Receive(ctx, m)
		}
	})
}

// isLZ4Compressed returns true if Content-Encoding is set to
// "lz4" in the given Message's Attributes.
//
// Note that lz4 is a non-standard coding for this header
// (i.e. it is not defined in RFC 2616:
// https://datatracker.ietf.org/doc/html/rfc2616#section-3.5).
//
// Note: MIMEHeader.Get() is used to fetch this value. In the case of a list of
// values, .Get() returns the 0th value.
func isLZ4Compressed(m *msg.Message) bool {
	return m.Attributes.Get("Content-Encoding") == "lz4"
}
