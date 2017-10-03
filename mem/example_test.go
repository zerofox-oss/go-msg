package mem_test

import (
	"context"
	"encoding/csv"
	"fmt"
	"sort"

	"github.com/zerofox-oss/go-msg"
	"github.com/zerofox-oss/go-msg/mem"
)

func Example_primitives() {
	c1 := make(chan *msg.Message, 10)
	c2 := make(chan *msg.Message, 10)
	c3 := make(chan *msg.Message, 10)

	t1, t2, t3 := mem.Topic{C: c1}, mem.Topic{C: c2}, mem.Topic{C: c3}
	srv1, srv2 := mem.NewServer(c1, 1), mem.NewServer(c2, 1)

	// split csv into separate messages for analysis
	go func() {
		srv1.Serve(msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			lines, err := csv.NewReader(m.Body).ReadAll()
			if err != nil {
				return err
			}

			for _, row := range lines {
				for _, col := range row {
					w := t2.NewWriter()
					w.Write([]byte(col))
					w.Close()
				}
			}
			return nil
		}))
	}()

	// perform some analysis on each message
	go func() {
		srv2.Serve(msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			body, err := msg.DumpBody(m)
			if err != nil {
				return err
			}

			w := t3.NewWriter()
			w.Attributes().Set("Length", fmt.Sprintf("%d", len(body)))
			w.Attributes().Set("StartsWith", string(body[0:1]))

			if len(body)%2 == 0 {
				w.Attributes().Set("Even", "true")
				w.Attributes().Set("Odd", "false")
			} else {
				w.Attributes().Set("Even", "false")
				w.Attributes().Set("Odd", "true")
			}
			w.Write(body)
			return w.Close()
		}))
	}()

	messages := [][]byte{
		[]byte("foo,bar,baz"),
		[]byte("one,two,three,four"),
	}

	for _, m := range messages {
		w := t1.NewWriter()
		w.Write(m)
		w.Close()
	}

	calls, expectedCalls := 0, 7

	for m := range c3 {
		orderedKeys := make([]string, 0)
		for k := range m.Attributes {
			orderedKeys = append(orderedKeys, k)
		}
		sort.Strings(orderedKeys)

		for _, k := range orderedKeys {
			fmt.Printf("%s:%v ", k, m.Attributes.Get(k))
		}
		fmt.Printf("%v\n", m.Body)

		calls++
		if calls == expectedCalls {
			close(c3)
		}
	}

	// Output:
	// Even:false Length:3 Odd:true Startswith:f foo
	// Even:false Length:3 Odd:true Startswith:b bar
	// Even:false Length:3 Odd:true Startswith:b baz
	// Even:false Length:3 Odd:true Startswith:o one
	// Even:false Length:3 Odd:true Startswith:t two
	// Even:false Length:5 Odd:true Startswith:t three
	// Even:true Length:4 Odd:false Startswith:f four
}
