package kafka

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaProducer(broker string, writerTopic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    writerTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

func writeToKafka(key string, payload any, writer *kafka.Writer) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Errore serializzazione JSON: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: jsonBytes,
	})

	if err != nil {
		log.Printf("Errore invio Kafka: %v", err)
		return err
	}

	return nil
}
