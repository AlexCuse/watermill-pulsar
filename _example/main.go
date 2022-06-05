package main

import (
	"context"
	"log"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-pulsar/pkg/pulsar"
	"github.com/ThreeDotsLabs/watermill/message"
)

func main() {
	purl := "pulsar://localhost:6650"
	logger := watermill.NewStdLogger(true, false)

	subscriber, err := pulsar.NewSubscriber(
		pulsar.SubscriberConfig{
			URL: purl,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	messages, err := subscriber.Subscribe(context.Background(), "example_topic_pulsar")
	if err != nil {
		panic(err)
	}

	go process(messages)

	publisher, err := pulsar.NewPublisher(
		pulsar.PublisherConfig{
			URL: purl,
		},
		logger,
	)
	if err != nil {
		panic(err)
	}

	publishMessages(publisher)
}

func publishMessages(publisher message.Publisher) {
	for {
		msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, world!"))

		if err := publisher.Publish("example_topic_pulsar", msg); err != nil {
			panic(err)
		}

		time.Sleep(time.Second)
	}
}

func process(messages <-chan *message.Message) {
	for msg := range messages {
		log.Printf("received message: %s, payload: %s", msg.UUID, string(msg.Payload))

		// we need to Acknowledge that we received and processed the message,
		// otherwise, it will be resent over and over again.
		msg.Ack()
	}
}
