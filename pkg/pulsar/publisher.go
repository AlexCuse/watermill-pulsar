package pulsar

import (
	"context"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"
)

// PublisherConfig is the configuration to create a publisher
type PublisherConfig struct {
	// URL is the Pulsar URL.
	URL string
}

// Publisher provides the pulsar implementation for watermill publish operations
type Publisher struct {
	conn pulsar.Client
	pubs map[string]pulsar.Producer

	logger watermill.LoggerAdapter
}

// NewPublisher creates a new Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	conn, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.URL,
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return NewPublisherWithPulsarClient(conn, logger)
}

// NewPublisherWithPulsarClient creates a new Publisher with the provided nats connection.
func NewPublisherWithPulsarClient(conn pulsar.Client, logger watermill.LoggerAdapter) (*Publisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Publisher{
		conn:   conn,
		logger: logger,
		pubs:   make(map[string]pulsar.Producer, 0),
	}, nil
}

// Publish publishes message to Pulsar.
//
// Publish will not return until an ack has been received from JetStream.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer, found := p.pubs[topic]

	if !found {
		pr, err := p.conn.CreateProducer(pulsar.ProducerOptions{Topic: topic})

		if err != nil {
			return err
		}

		producer = pr
		p.pubs[topic] = producer
	}

	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		producer.Send(ctx, &pulsar.ProducerMessage{
			Key:     msg.UUID,
			Payload: msg.Payload,
		})
	}

	return nil
}

// Close closes the publisher and the underlying connection
func (p *Publisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("Publisher closed", nil)

	for _, pub := range p.pubs {
		pub.Close()
	}

	p.conn.Close()

	return nil
}
