package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"progettoSDCC/events-generator/package/config"
	"progettoSDCC/events-generator/package/sensor"
	"time"

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
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Printf("Errore lettura Kafka: %v", err)
				continue
			}

			var cmd config.EmergencyCommand
			if err := json.Unmarshal(msg.Value, &cmd); err != nil {
				log.Printf("[ERROR] Messaggio malformato ricevuto su Kafka: %v. Scarto.", err)
				_ = reader.CommitMessages(ctx, msg) // Commit per evitare blocco dell'offset
				continue
			}

			// Gestiamo l'evento in base al tipo di comando
			if cmd.Command == config.ModeStop {
				fmt.Printf("[KAFKA CONSUMER] Ricevuto STOP per %s\n", cmd.SensorID)

				success := false
				maxRetries := 3

				for attempt := 1; attempt <= maxRetries; attempt++ {
					if sensor.SendControlCommand(cmd.SensorID, config.StateCommand{Mode: config.ModeStop}) {
						success = true
						break
					}

					log.Printf("[WARNING] Tentativo %d/%d invio STOP fallito per sensore %s", attempt, maxRetries, cmd.SensorID)

					//	Attendo il timer prima di eseguire un nuovo retry
					select {
					case <-ctx.Done():
						return
					case <-time.After(time.Duration(attempt*100) * time.Millisecond):
					}
				}

				if !success {
					log.Printf("[CRITICAL ERROR] Impossibile inviare lo STOP al sensore %s (non presente nel registry o canale pieno)", cmd.SensorID)
				}
			}

			// COMMIT SEMPRE ESEGUITO: sia in caso di success, sia di fallimento/comando ignorato
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("Errore commit offset Kafka: %v", err)
			}
		}
	}()
}
