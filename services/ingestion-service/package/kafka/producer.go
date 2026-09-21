package kafka

import (
	"context"
	"encoding/json"
	"ingestion-service/package/config"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaProducer(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.Hash{}, // Hash garantisce che i messaggi dello stesso sensore finiscano nella stessa partizione
	}
}

func writeEventToKafka(sensorID string, event config.MessageStreamEvent, writer *kafka.Writer) error {
	jsonBytes, err := json.Marshal(event)
	if err != nil {
		log.Printf("Errore serializzazione MessageStreamEvent JSON: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(sensorID),
		Value: jsonBytes,
	})

	if err != nil {
		log.Printf("Errore invio Kafka: %v", err)
		return err
	}

	return nil
}
