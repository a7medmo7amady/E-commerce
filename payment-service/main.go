package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/IBM/sarama"
)

const (
	paymentCommandsTopic = "payment-commands"
	orderEventsTopic     = "order-events"
	paymentDLQTopic      = "payment-dlq"
	maxRetries           = 3
)

func main() {
	rand.Seed(time.Now().UnixNano())

	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	producer, err := newSyncProducer(broker)
	if err != nil {
		log.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{broker}, "payment-service-group", config)
	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}
	defer group.Close()

	handler := &PaymentHandler{
		producer: producer,
	}

	log.Println("Payment Service started")

	ctx := context.Background()

	for {
		if err := group.Consume(ctx, []string{paymentCommandsTopic}, handler); err != nil {
			log.Printf("consumer error: %v", err)
			time.Sleep(2 * time.Second)
		}
	}
}

type PaymentHandler struct {
	producer sarama.SyncProducer
}

func (h *PaymentHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *PaymentHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *PaymentHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var cmd map[string]any
		if err := json.Unmarshal(msg.Value, &cmd); err != nil {
			log.Printf("invalid command: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		log.Printf("Received command: %s", string(msg.Value))

		ok := h.processWithRetry(cmd)
		if !ok {
			if err := publish(h.producer, paymentDLQTopic, getString(cmd, "orderId"), cmd); err != nil {
				log.Printf("failed to publish to DLQ: %v", err)
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *PaymentHandler) processWithRetry(cmd map[string]any) bool {
	orderID := getString(cmd, "orderId")
	userID := getString(cmd, "userId")
	amount := getFloat(cmd, "amount")

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := processPayment(orderID, amount)
		if err == nil {
			event := map[string]any{
				"eventId": generateID("evt"),
				"orderId": orderID,
				"userId":  userID,
				"amount":  amount,
				"type":    "PaymentCompleted",
				"message": "payment processed successfully",
			}
			if err := publish(h.producer, orderEventsTopic, orderID, event); err != nil {
				log.Printf("failed to publish success event: %v", err)
			}
			return true
		}

		log.Printf("payment attempt %d failed for order %s: %v", attempt, orderID, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	event := map[string]any{
		"eventId": generateID("evt"),
		"orderId": orderID,
		"userId":  userID,
		"amount":  amount,
		"type":    "PaymentFailed",
		"message": "payment failed after retries",
	}
	if err := publish(h.producer, orderEventsTopic, orderID, event); err != nil {
		log.Printf("failed to publish failure event: %v", err)
	}

	return false
}

func processPayment(orderID string, amount float64) error {
	_ = amount

	time.Sleep(2 * time.Second)

	if rand.Intn(10) < 7 {
		log.Printf("payment success for order %s", orderID)
		return nil
	}

	return ErrTemporaryFailure
}