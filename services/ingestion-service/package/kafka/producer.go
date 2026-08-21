package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func newKafkaProducer(broker, topic string) *kafka.Writer {
	kafkaProducer := kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &kafkaProducer
}

func writeToKafka(payload any, writer *kafka.Writer) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Errore serializzazione JSON: %v", err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = writer.WriteMessages(ctx, kafka.Message{
		Value: jsonBytes,
	})

	if err != nil {
		log.Printf("Errore invio Kafka: %v", err)
		return err
	}

	fmt.Printf("Messaggio pulito inoltrato: %s\n", string(jsonBytes))
	return nil
}
