package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewKafkaConsumer(broker string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    "processed-data-topic",
		GroupID:  "sink-service-group",
		MinBytes: 10,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
}
