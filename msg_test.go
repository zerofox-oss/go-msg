package msg_test

import (
	"context"
	"io/ioutil"
	"net/textproto"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	msg "github.com/zerofox-oss/go-msg"
)

const expected = "hello world"

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func TestGetAttribute(t *testing.T) {
	a := msg.Attributes{}

	// doesn't return if nothing is there
	if v := a.Get("foo"); v != "" {
		t.Errorf("expected nothing, got %s", v)
	}

	// returns if something is there
	a.Set("foo", "bar")
	if v := a.Get("foo"); v != "bar" {
		t.Errorf("expected bar, got %s", v)
	}

	// returns if something is there (case is different)
	if v := a.Get("FOO"); v != "bar" {
		t.Errorf("expected bar, got %s", v)
	}
}

func TestSetAttribute(t *testing.T) {
	a := msg.Attributes{}

	// if k/v not set, set it
	k := textproto.CanonicalMIMEHeaderKey("foo")

	a.Set("foo", "bar")
	if v := a[k]; !reflect.DeepEqual(v, []string{"bar"}) {
		t.Errorf("expected bar, got %s", v)
	}

	// if same key, override value
	a.Set("foo", "baz")
	if v := a[k]; !reflect.DeepEqual(v, []string{"baz"}) {
		t.Errorf("expected baz, got %s", v)
	}

	// if same key (different case), override value
	k = textproto.CanonicalMIMEHeaderKey("FOO")

	a.Set("FOO", "bin")
	if v := a[k]; !reflect.DeepEqual(v, []string{"bin"}) {
		t.Errorf("expected bin, got %s", v)
	}
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
		Body:       strings.NewReader("hello world"),
	}
	m.Attributes.Set("foo", "bar")

	mm := msg.WithBody(m, strings.NewReader("hello new world"))
	body, err := msg.DumpBody(mm)
	if err != nil {
		t.Fatal(err)
	}

	// assert attributes are copied but body is new
	if mm.Attributes.Get("foo") != "bar" {
		t.Errorf("Attributes failed to copy")
	}
	if string(body) != "hello new world" {
		t.Errorf("body does not match expected %s", string(body))
	}

	// assert that message attributes are not shared
	m.Attributes.Set("test", "one")
	mm.Attributes.Set("test", "two")

	if m.Attributes.Get("test") == mm.Attributes.Get("test") {
		t.Errorf("message attributes should not be the same")
	}
}

type testMessageWriter struct {
	attrs msg.Attributes
	delay time.Duration
}

func (t *testMessageWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (t *testMessageWriter) Close() error {
	return nil
}

func (t *testMessageWriter) Attributes() *msg.Attributes {
	return &t.attrs
}

func (t *testMessageWriter) SetDelay(d time.Duration) {
	t.delay = d
}

func TestTopicFuncNewWriter(t *testing.T) {
	w := &testMessageWriter{}
	tf := msg.TopicFunc(func(ctx context.Context) msg.MessageWriter {
		return w
	})

	mw := tf.NewWriter(context.Background())
	if mw != w {
		t.Error("TopicFunc did not return expected MessageWriter")
	}

	expectedDelay := 5 * time.Second
	mw.SetDelay(expectedDelay)
	if w.delay != expectedDelay {
		t.Errorf("SetDelay(%v) = %v, want %v", expectedDelay, w.delay, expectedDelay)
	}
}
