package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"progettoSDCC/events-generator/package/config"
	"progettoSDCC/events-generator/package/sensor"

	"github.com/segmentio/kafka-go"
)

func StartEmergencyConsumer(ctx context.Context, broker, topic, groupID string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6,
	})

	go func() {
		defer func(reader *kafka.Reader) {
			_ = reader.Close()
		}(reader)
		fmt.Printf("Consumer attivo su topic '%s'...\n", topic)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					continue
				}

				var cmd config.EmergencyCommand
				if err := json.Unmarshal(msg.Value, &cmd); err != nil {
					continue
				}

				if cmd.Command == config.ModeStop {
					fmt.Printf("[KAFKA CONSUMER] Ricevuto STOP per %s\n", cmd.MachineID)
					sensor.SendControlCommand(cmd.MachineID, config.StateCommand{Mode: config.ModeStop})
				}
			}
		}
	}()
}
