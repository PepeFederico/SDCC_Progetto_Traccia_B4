package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

func NewKafkaConsumer(broker string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    "data-topic-cleaned",
		GroupID:  "analyzer-service-group",
		MinBytes: 10,
		MaxBytes: 10e6,
		MaxWait:  10 * time.Millisecond,
	})
}
