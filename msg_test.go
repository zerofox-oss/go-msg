package msg_test

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	msg "github.com/zerofoxlabs/go-msg"
)

const expected = "hello world"

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestDumpBody(t *testing.T) {
	m := &msg.Message{
		Body: strings.NewReader(expected),
	}
	b, err := msg.DumpBody(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != expected {
		t.Errorf("Dumped body does not match expected: %s != %s", expected, string(b))
	}
}

func TestCloneBody(t *testing.T) {
	m := &msg.Message{
		Body: strings.NewReader(expected),
	}
	b, err := msg.CloneBody(m)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := ioutil.ReadAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if string(bb) != expected {
		t.Errorf("Cloned body does not match expected: %s != %s", expected, string(bb))
	}
}

func TestWithBody(t *testing.T) {
	m := &msg.Message{
		Attributes: msg.Attributes{},
		Body:       strings.NewReader("world hello"),
	}
	// This panics if m.Attributes is nil
	m.Attributes.Set("hello", "world")
	b, err := msg.DumpBody(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == expected {
		t.Errorf("Dumped body shouldn't match expected, but: %s == %s", expected, string(b))
	}
	mm := msg.WithBody(m, strings.NewReader(expected))
	bb, err := msg.DumpBody(mm)
	if err != nil {
		t.Fatal(err)
	}
	if string(bb) != expected {
		t.Errorf("Dumped body does not match expected: %s != %s", expected, string(bb))
	}
	if mm.Attributes.Get("hello") != "world" {
		t.Errorf("Attributes failed to copy")
	}
	mm.Attributes.Set("test", "one")
	m.Attributes.Set("test", "two")

	if m.Attributes.Get("test") == mm.Attributes.Get("test") {
		t.Errorf("The two message attributes should not affect each other %s == %s",
			m.Attributes.Get("test"),
			mm.Attributes.Get("test"))
	}
}
