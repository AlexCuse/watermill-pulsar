package pulsar

import (
	"context"
	"errors"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/apache/pulsar-client-go/pulsar"
	"sync"
)

// SubscriberConfig is the configuration to create a subscriber
type SubscriberConfig struct {
	// URL is the URL to the broker
	URL string
}

// SubscriptionConfig is the configuration to customize the subscription behaviour.
type SubscriptionConfig struct {
	// SubscriptionName is the name of the subscription.
	SubscriptionName string
	// SubscriptionType is the type of the subscription.
	SubscriptionType pulsar.SubscriptionType
}

// Subscriber provides the pulsar implementation for watermill subscribe operations
type Subscriber struct {
	conn   pulsar.Client
	logger watermill.LoggerAdapter

	subsLock sync.RWMutex
	subs     map[string]pulsar.Consumer
	closed   bool
	closing  chan struct{}

	outputsWg        sync.WaitGroup
	SubscribersCount int
	clientID         string
}

// NewSubscriber creates a new Subscriber.
func NewSubscriber(config SubscriberConfig, logger watermill.LoggerAdapter) (*Subscriber, error) {
	conn, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: config.URL,
	})
	if err != nil {
		return nil, errors.Join(err, errors.New("cannot connect to Pulsar"))
	}
	return NewSubscriberWithPulsarClient(conn, logger)
}

// NewSubscriberWithPulsarClient creates a new Subscriber with the provided pulsar client.
func NewSubscriberWithPulsarClient(conn pulsar.Client, logger watermill.LoggerAdapter) (*Subscriber, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	return &Subscriber{
		conn:     conn,
		logger:   logger,
		closing:  make(chan struct{}),
		clientID: watermill.NewULID(),
		subs:     make(map[string]pulsar.Consumer),
	}, nil
}

// Subscribe subscribes messages from Pulsar.
func (s *Subscriber) Subscribe(ctx context.Context, topic string, config SubscriptionConfig) (<-chan *message.Message, error) {
	output := make(chan *message.Message)

	s.subsLock.Lock()
	defer s.subsLock.Unlock()

	sub, found := s.subs[topic]

	subscriptionName := fmt.Sprintf("%s-%s", topic, s.clientID)
	if config.SubscriptionName != "" {
		subscriptionName = config.SubscriptionName
	}

	if !found {
		sb, err := s.conn.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: subscriptionName,
			Type:             config.SubscriptionType,
			MessageChannel:   make(chan pulsar.ConsumerMessage, 10),
		})

		if err != nil {
			return nil, err
		}

		s.subs[topic] = sb
		sub = sb
	}

	go func() {
		for !s.isClosed() {
			select {
			case <-ctx.Done():
				s.logger.Info("exiting on context closure", nil)
				return
			case m := <-sub.Chan():
				go s.processMessage(ctx, output, m, sub)
			}
		}
	}()

	return output, nil
}

func (s *Subscriber) processMessage(ctx context.Context, output chan *message.Message, m pulsar.Message, sub pulsar.Consumer) {
	if s.isClosed() {
		return
	}

	logFields := watermill.LogFields{}
	s.logger.Trace("Received message", logFields)

	ctx, cancelCtx := context.WithCancel(ctx)
	defer cancelCtx()

	messageLogFields := logFields.Add(watermill.LogFields{"message_uuid": m.Key()})
	s.logger.Trace("Unmarshaled message", messageLogFields)

	msg := &message.Message{
		UUID:    m.Key(),
		Payload: m.Payload(),
	}

	select {
	case <-s.closing:
		s.logger.Trace("Closing, message discarded", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded", messageLogFields)
		return
	// if this is first can risk 'send on closed channel' errors
	case output <- msg:
		s.logger.Trace("Message sent to consumer", messageLogFields)
	}

	select {
	case <-msg.Acked():
		sub.Ack(m)
		s.logger.Trace("Message Acked", messageLogFields)
	case <-msg.Nacked():
		sub.Nack(m)
		s.logger.Trace("Message Nacked", messageLogFields)
	case <-s.closing:
		s.logger.Trace("Closing, message discarded before ack", messageLogFields)
		return
	case <-ctx.Done():
		s.logger.Trace("Context cancelled, message discarded before ack", messageLogFields)
		return
	}
}

// Close closes the publisher and the underlying connection.  It will attempt to wait for in-flight messages to complete.
func (s *Subscriber) Close() error {
	s.subsLock.Lock()
	defer s.subsLock.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	s.logger.Debug("Closing subscriber", nil)
	defer s.logger.Info("Subscriber closed", nil)

	close(s.closing)

	for _, sub := range s.subs {
		sub.Close()
	}
	s.conn.Close()

	return nil
}

func (s *Subscriber) isClosed() bool {
	s.subsLock.RLock()
	defer s.subsLock.RUnlock()

	return s.closed
}
