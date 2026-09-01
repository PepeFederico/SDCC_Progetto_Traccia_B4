package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"progettoSDCC/events-generator/package/config"

	"github.com/segmentio/kafka-go"
)

func NewWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(broker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func SimulationReceiveMessage(kafkaWriter *kafka.Writer, machineID string) {
	payload := config.EmergencyCommand{
		SensorID: machineID,
		Command:  config.ModeStop,
	}

	jsonBytes, _ := json.Marshal(payload)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_ = kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(machineID),
		Value: jsonBytes,
	})

	fmt.Printf("[SEGNALE DAL SISTEMA --> %s] Inviato comando STOP su Kafka\n", machineID)
}
