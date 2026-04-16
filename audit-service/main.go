package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type AuditHandler struct{}

func (h *AuditHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *AuditHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *AuditHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("[AUDIT] topic=%s partition=%d offset=%d payload=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	group, err := sarama.NewConsumerGroup([]string{broker}, "audit-service-group", config)
	if err != nil {
		log.Fatalf("failed to create audit consumer group: %v", err)
	}
	defer group.Close()

	handler := &AuditHandler{}
	log.Println("Audit Service started")

	ctx := context.Background()

	for {
		if err := group.Consume(ctx, []string{"order-events"}, handler); err != nil {
			log.Printf("audit consumer error: %v", err)
			time.Sleep(2 * time.Second)
		}
	}
}