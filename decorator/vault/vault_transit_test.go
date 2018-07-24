package vault

import (
	"bytes"
	"context"
	"encoding/base64"
	"io/ioutil"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/builtin/logical/transit"
	"github.com/hashicorp/vault/http"
	"github.com/hashicorp/vault/vault"

	"github.com/riskive/go-msg"
	"github.com/riskive/go-msg/mem"
)

func TestTransit_Decrypter(t *testing.T) {
	vaultClient, done := getTestVaultClient(t)
	defer done()

	inCh := make(chan *msg.Message, 1)
	Transit := &Transit{
		Client: vaultClient,
	}
	decrypter := Transit.Decrypter(msg.ReceiverFunc(func(_ context.Context, m *msg.Message) error {
		inCh <- m
		return nil
	}))
	expectedBody := "dannunz"

	secret, err := vaultClient.Logical().Write("transit/encrypt/testkey", map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString([]byte(expectedBody)),
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	encryptedMessage := &EncryptedMessage{
		Backend:    "",
		Key:        "testkey",
		CipherText: secret.Data["ciphertext"].(string),
	}

	protoBytes, err := proto.Marshal(encryptedMessage)
	if err != nil {
		t.Fatal(err.Error())
	}

	messageToReceive := &msg.Message{
		Attributes: msg.Attributes{},
		Body:       bytes.NewBuffer(protoBytes),
	}

	err = decrypter.Receive(context.Background(), messageToReceive)
	if err != nil {
		t.Fatal(err.Error())
	}

	select {
	case m := <-inCh:
		messageBody, err := ioutil.ReadAll(m.Body)
		if err != nil {
			t.Fatal(err.Error())
		}
		messageBodyStr := string(messageBody)
		if messageBodyStr != expectedBody {
			t.Fatalf("Expected body %s, saw %s", expectedBody, messageBodyStr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestTransit_Encrypter(t *testing.T) {
	outCh := make(chan *msg.Message, 1)
	next := &mem.Topic{
		OutChan: outCh,
	}

	vaultClient, done := getTestVaultClient(t)
	defer done()

	Transit := &Transit{
		Key:    "testkey",
		Client: vaultClient,
	}

	encrypter := Transit.Encrypter(next)
	encryptedWriter := encrypter.NewWriter()
	bytesToWrite := []byte{'h', 'e', 'l', 'l', 'o'}

	// check supports multi-write
	for _, b := range bytesToWrite {
		if _, err := encryptedWriter.Write([]byte{b}); err != nil {
			t.Fatalf("Unexpected error in w.Write(): %s", err.Error())
		}
	}

	if err := encryptedWriter.Close(); err != nil {
		t.Fatalf("Unexpected error in w.Close(): %s", err.Error())
	}

	select {
	case m := <-outCh:
		body, err := ioutil.ReadAll(m.Body)
		if err != nil {
			t.Fatal(err.Error())
		}

		protoMsg := &EncryptedMessage{}
		if err := proto.Unmarshal(body, protoMsg); err != nil {
			t.Fatal(err.Error())
		}
		if protoMsg.Key != "testkey" {
			t.Fatalf("Expected Key %s, saw %s", "testkey", protoMsg.Key)
		}
		if protoMsg.Backend != "" {
			t.Fatalf("Unexpected Backend %s", protoMsg.Backend)
		}

		secret, err := vaultClient.Logical().Write("transit/decrypt/testkey", map[string]interface{}{
			"ciphertext": protoMsg.CipherText,
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		decryptedBody, err := base64.StdEncoding.DecodeString(secret.Data["plaintext"].(string))
		if err != nil {
			t.Fatal(err.Error())
		}
		if !bytes.Equal(decryptedBody, bytesToWrite) {
			t.Fatalf("Expected body %s, saw %s", bytesToWrite, decryptedBody)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Test timed out")
	}
}

func TestTransitWriter_Attributes(t *testing.T) {
	next := (&mem.Topic{}).NewWriter()

	w := &encryptWriter{
		Next: next,
	}

	if w.Attributes() != next.Attributes() {
		t.Fatal("Attributes not equal")
	}
}

func getTestVaultClient(t *testing.T) (*api.Client, func()) {
	if err := vault.AddTestLogicalBackend("transit", transit.Factory); err != nil {
		t.Fatal(err.Error())
	}

	core, _, token := vault.TestCoreUnsealed(t)

	ln, addr := http.TestServer(t, core)

	http.TestServerAuth(t, addr, token)

	c, err := api.NewClient(&api.Config{
		Address: addr,
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	c.SetToken(token)

	if err := c.Sys().Mount("transit", &api.MountInput{
		Type:        "transit",
		Description: "transit",
		Config: api.MountConfigInput{
			DefaultLeaseTTL: "288h",
			MaxLeaseTTL:     "2880h",
		},
	}); err != nil {
		t.Fatal(err.Error())
	}

	if _, err := c.Logical().Write("transit/keys/testkey", map[string]interface{}{
		"type": "aes256-gcm96",
	}); err != nil {
		t.Fatal(err.Error())
	}

	return c, func() {
		ln.Close()
	}
}
