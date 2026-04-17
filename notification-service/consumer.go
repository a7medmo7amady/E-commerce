package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type PaymentEvent struct {
	EventID  string  `json:"eventId"`
	OrderID  string  `json:"orderId"`
	UserID   string  `json:"userId"`
	Amount   float64 `json:"amount"`
	Type     string  `json:"type"`
	Message  string  `json:"message"`
}

type Consumer struct {
	group    sarama.ConsumerGroup
	notifier *Notifier
	cancel   context.CancelFunc
}

func NewConsumer(broker string, notifier *Notifier) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{broker}, "notification-service-group", config)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{group: group, notifier: notifier, cancel: cancel}

	go func() {
		for {
			err := group.Consume(ctx, []string{"order-events"}, c)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("consumer error: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()

	return c, nil
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event PaymentEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("failed to parse event: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		c.notifier.Notify(event)
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) Close() {
	c.cancel()
	c.group.Close()
}
