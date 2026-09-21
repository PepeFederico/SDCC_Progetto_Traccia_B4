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
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    100,                   // Invia quando accumula 100 messaggi
		BatchTimeout: 50 * time.Millisecond, // Oppure invia ogni 10ms
		Async:        true,                  // Scrittura non bloccante per la goroutine chiamante
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
