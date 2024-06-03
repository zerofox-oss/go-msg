package main

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zerofox-oss/go-msg"
)

// $ brew upgrade redis
// $ redis-server -v
// Redis server v=7.2.5 sha=00000000:0 malloc=libc bits=64 build=bd81cd1340e80580
//
// Note, redis 7 is compatible with go-redis/v9, but not go-redis/v8
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

	shutdownCtx, _ := context.WithTimeout(ctx, 6*time.Second)
	multipleConsumers(shutdownCtx, client)
}

func multipleConsumers(ctx context.Context, client *redis.Client) {
	log.Printf("=== Test #1 - Multiple Consumers of a Stream")

	stream := "stream1"
	groups := []string{"group1", "group2"}

	// create stream + consumer groups (we'll make 3 to show we can have multiple copies)
	resp0 := client.Del(ctx, stream)
	if err := resp0.Err(); err != nil {
		log.Fatalf("DEL %s failed with: %s", stream, err)
	}

	// Consumer groups should only receive new messages - therefore specify $ as the ID
	// Seehttps://redis.io/docs/latest/commands/xgroup-create/
	for _, group := range groups {
		resp := client.XGroupCreateMkStream(ctx, stream, group, "$")
		if resp.Err() != nil {
			log.Fatalf("could not create consumer group (%s): %s", group, resp.Err())
		}
	}

	resp1 := client.XInfoStream(ctx, stream)
	if err := resp1.Err(); err != nil {
		log.Fatalf("XInfoStream failed with: %s", err)
	}

	// write messages to stream
	topic := Topic{Stream: stream, Conn: client}
	messages := []struct {
		Key   string
		Value string
	}{
		{
			Key:   "Hello",
			Value: "World",
		},
		{
			Key:   "This is",
			Value: "A message",
		},
	}

	for _, message := range messages {
		bytes, err := json.Marshal(message)
		if err != nil {
			log.Fatalf("could not marshal message: %s", err)
		}

		w := topic.NewWriter(ctx)
		if _, err := w.Write(bytes); err != nil {
			log.Fatalf("could not write message: %s", err)
		}

		if err := w.Close(); err != nil {
			log.Fatalf("could not close message: %s", err)
		}
	}

	log.Printf("Stream=%s has messages=%d, consumer_groups=%d", stream, len(messages), resp1.Val().Groups)

	// setup consumers for each group
	var count atomic.Uint32

	go func() {
		srv1, err := NewServer(client, stream, "group1", "consumer1")
		if err != nil {
			log.Fatalf("could not start server: %s", err)
		}

		receiveFunc := msg.ReceiverFunc(func(ctx context.Context, m *msg.Message) error {
			log.Printf("Returning without error to ACK.")
			count.Add(1)

			return nil
		})

		// sleep to allow the loop below to start
		time.Sleep(2 * time.Second)
		srv1.Serve(receiveFunc)
	}()

	for {
		select {
		case <-ctx.Done():
			if count.Load() == uint32(2) {
				log.Printf("Processed all messages!")
			}

			return
		default:
			resp := client.XInfoGroups(ctx, stream)
			if err := resp.Err(); err != nil {
				log.Fatalf("XInfoGroups failed with: %s", err)
			}

			log.Printf("XInfoGroups: %+v", resp.Val())
			time.Sleep(1 * time.Second)
		}
	}
}
