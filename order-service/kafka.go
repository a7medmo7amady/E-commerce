package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type KafkaProducer struct {
	producer sarama.SyncProducer
}

func NewKafkaProducer() (*KafkaProducer, error) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Timeout = 5 * time.Second

	p, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: p}, nil
}

func (kp *KafkaProducer) Publish(topic string, key string, payload any) error {
	bytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(bytes),
	}

	partition, offset, err := kp.producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Published to topic=%s partition=%d offset=%d\n", topic, partition, offset)
	return nil
}

func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}

