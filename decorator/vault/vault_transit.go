package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"path"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/vault/api"

	"github.com/riskive/go-msg"
)

// Transit represents Vault-aware decorators for
// encrypting messages to send to a msg.Topic
// and decrypting messages to send to a msg.Receiver
// utilizing a Protobuf message that defines encryption data
type Transit struct {
	Backend string
	Key     string

	*api.Client
}

type encryptWriter struct {
	Next msg.MessageWriter
	*Transit

	buf bytes.Buffer
}

// Decrypter unmarshals msg.Message (EncryptedMessage protobuf)
// and utilizes the Vault Transit API to replace the message's
// body with unencrypted bytes to be received by the decorated Receiver
func (t *Transit) Decrypter(next msg.Receiver) msg.Receiver {
	return msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			b, err := msg.DumpBody(m)
			if err != nil {
				return nil
			}

			e := &EncryptedMessage{}

			if err := proto.Unmarshal(b, e); err != nil {
				return err
			}

			p := path.Join("transit", e.Backend, "decrypt", e.Key)

			s, err := t.Client.Logical().Write(p, map[string]interface{}{
				"ciphertext": e.CipherText,
			})
			if err != nil {
				return err
			}

			t, err := base64.StdEncoding.DecodeString(s.Data["plaintext"].(string))
			if err != nil {
				return err
			}

			// Set body to unencrypted bytes
			m.Body = bytes.NewBuffer(t)

			return next.Receive(ctx, m)
		}
	})
}

// Encrypter returns a Topic whose MessageWriter's
// utilize the Vault Transit API to encrypt a msg.Message's Body
// and marshals into a EncryptedMessage protobuf
// to write to the decorated Topic
func (t *Transit) Encrypter(next msg.Topic) msg.Topic {
	return msg.TopicFunc(func() msg.MessageWriter {
		return &encryptWriter{
			Next:    next.NewWriter(),
			Transit: t,
		}
	})
}

func (w *encryptWriter) Write(b []byte) (int, error) {
	return w.buf.Write(b)
}

func (w *encryptWriter) Close() error {
	p := path.Join("transit", w.Transit.Backend, "encrypt", w.Transit.Key)

	s, err := w.Client.Logical().Write(p, map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(w.buf.Bytes()),
	})
	if err != nil {
		return err
	}

	e := &EncryptedMessage{
		Backend:    w.Backend,
		Key:        w.Key,
		CipherText: s.Data["ciphertext"].(string),
	}

	m, err := proto.Marshal(e)
	if err != nil {
		return err
	}

	if _, err := w.Next.Write(m); err != nil {
		return err
	}

	return w.Next.Close()
}

func (w *encryptWriter) Attributes() *msg.Attributes {
	return w.Next.Attributes()
}
