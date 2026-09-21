package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"ingestion-service/package/config"

	"github.com/segmentio/kafka-go"
)

// StartSensorDataConsumer : Consumer Generico con Generics
func StartSensorDataConsumer[T any, PT interface {
	*T
	config.SensorData
}](
	ctx context.Context,
	broker string,
	readerTopic string,
	writerTopic string,
	groupID string,
	validate func(float64) bool,
) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    readerTopic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6,
	})

	writer := newKafkaProducer(broker, writerTopic)

	go func() {
		defer func() {
			_ = reader.Close()
			_ = writer.Close()
		}()

		fmt.Printf("Consumer attivo su Topic '%s'...\n", readerTopic)

		for {
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Errore lettura (%s): %v", readerTopic, err)
				continue
			}

			// 1. Parsing dell'inviluppo primario MessageStreamEvent
			var event config.MessageStreamEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Errore unmarshal MessageStreamEvent (%s): %v", readerTopic, err)
				continue
			}

			// -----------------------------------------------------------------
			// CASO A: Gestione del LATENCY_MARKER
			// -----------------------------------------------------------------
			if event.Type == "LATENCY_MARKER" && event.Marker != nil {
				// Inoltriamo il marker intatto sul topic di uscita "data-topic-cleaned"
				err := writer.WriteMessages(ctx, kafka.Message{
					Key:   msg.Key,
					Value: msg.Value,
				})
				if err != nil {
					log.Printf("Errore inoltro LATENCY_MARKER da %s a %s: %v", readerTopic, writerTopic, err)
				}
				continue
			}

			// -----------------------------------------------------------------
			// CASO B: Gestione del DATO SENSORIALE (DATA)
			// -----------------------------------------------------------------
			if event.Type == "DATA" && len(event.Payload) > 0 {
				var data T
				ptrData := PT(&data)

				// Unmarshal del sotto-payload specifico (TemperatureReading o PressureReading)
				if err := json.Unmarshal(event.Payload, ptrData); err != nil {
					log.Printf("Errore unmarshal payload JSON (%s): %v", readerTopic, err)
					continue
				}

				// Validazione tramite la funzione passata come parametro
				measure := ptrData.GetMeasure()
				if validate != nil && !validate(measure) {
					log.Printf("Misura fuori scala scartata su %s: %f", readerTopic, measure)
					continue
				}

				// Aggiornamento Timestamp
				ptrData.SetTimestamp(time.Now().UTC().Format(time.RFC3339))

				// Ricostruzione dell'inviluppo aggiornato prima di scrivere su Kafka
				updatedPayloadBytes, err := json.Marshal(ptrData)
				if err != nil {
					log.Printf("Errore marshal payload aggiornato: %v", err)
					continue
				}

				event.Payload = updatedPayloadBytes

				// Scrittura dell'evento pulito sul topic "data-topic-cleaned"
				if err := writeEventToKafka(ptrData.GetSensorID(), event, writer); err != nil {
					log.Printf("Errore invio messaggio su %s: %v", writerTopic, err)
				}
			}
		}
	}()
}
