package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	notifier := NewNotifier()
	consumer, err := NewConsumer(broker, notifier)
	if err != nil {
		log.Fatalf("failed to create consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("Notification Service started, listening for order events...")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Notification Service shutting down...")
}
