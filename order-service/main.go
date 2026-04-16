package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

func generateID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func startOrderEventConsumer(store *OrderStore) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{broker}, "order-service-group", config)
	if err != nil {
		log.Fatalf("failed to create order event consumer group: %v", err)
	}

	handler := &OrderEventHandler{store: store}
	ctx := context.Background()

	go func() {
		for {
			err := group.Consume(ctx, []string{"order-events"}, handler)
			if err != nil {
				log.Printf("order event consumer error: %v", err)
				time.Sleep(2 * time.Second)
			}
		}
	}()
}

type OrderEventHandler struct {
	store *OrderStore
}

func (h *OrderEventHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *OrderEventHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *OrderEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event map[string]any
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("failed to parse event: %v", err)
			session.MarkMessage(msg, "")
			continue
		}

		orderID, _ := event["orderId"].(string)
		eventType, _ := event["type"].(string)

		switch eventType {
		case "PaymentCompleted":
			h.store.UpdateStatus(orderID, "PAID")
		case "PaymentFailed":
			h.store.UpdateStatus(orderID, "FAILED")
		}

		log.Printf("Order Service updated order %s due to event %s", orderID, eventType)
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	store := NewOrderStore()

	producer, err := NewKafkaProducer()
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	defer producer.Close()

	startOrderEventConsumer(store)

	r := gin.Default()
	RegisterRoutes(r, store, producer)

	log.Println("Order Service running on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}                 