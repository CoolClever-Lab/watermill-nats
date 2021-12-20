package jetstream

import (
	"bytes"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
)

type Subscriber interface {
	Subscribe(subj string) (<-chan *message.Message, error)
	SubscribeSync(subj string) error
}

type SubscriberConfig struct {
	RawConnectionConfig
}

type JetStreamSubscriber struct {
	conn    nats.JetStreamContext
	logger  watermill.LoggerAdapter
	options []nats.SubOpt
}

func NewSubscriber(config *SubscriberConfig, logger watermill.LoggerAdapter, options ...nats.SubOpt) (Subscriber, error) {
	js, err := NewRawConnection(&config.RawConnectionConfig)
	if err != nil {
		return nil, err
	}

	return &JetStreamSubscriber{
		conn:    js,
		logger:  logger,
		options: options,
	}, nil
}

func (s *JetStreamSubscriber) Subscribe(subj string) (<-chan *message.Message, error) {
	out := make(chan *message.Message)

	if _, err := s.conn.Subscribe(subj, func(m *nats.Msg) {
		buf := bytes.NewBuffer(m.Data)

		var msg message.Message

		if err := json.NewDecoder(buf).Decode(&msg); err != nil {
			s.logger.Error("Can't decode to message struct", err, nil)
		}
	}, s.options...); err != nil {
		return nil, err
	}

	return out, nil
}

func (s *JetStreamSubscriber) SubscribeSync(subj string) error {
	_, err := s.conn.SubscribeSync(subj, s.options...)
	if err != nil {
		return err
	}

	return nil
}
