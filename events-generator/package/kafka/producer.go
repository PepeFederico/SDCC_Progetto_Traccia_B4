package kafka

import (
	"github.com/segmentio/kafka-go"
)

func NewWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
}

func NewWriterLatencyMarker(broker string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Balancer: &kafka.Hash{},
	}
}
