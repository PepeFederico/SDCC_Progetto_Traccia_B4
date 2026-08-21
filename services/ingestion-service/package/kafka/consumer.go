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

			// Istanziazione del tipo corretto (TemperatureReading o PressureReading)
			var data T
			ptrData := PT(&data)

			// Parsing JSON sul tipo specifico
			if err := json.Unmarshal(msg.Value, ptrData); err != nil {
				log.Printf("Errore unmarshal JSON (%s): %v", readerTopic, err)
				continue
			}

			// Validazione tramite l'interfaccia
			measure := ptrData.GetMeasure()
			if validate != nil && !validate(measure) {
				log.Printf("Misura fuori scala scartata su %s: %f", readerTopic, measure)
				continue
			}

			// Aggiornamento Timestamp
			ptrData.SetTimestamp(time.Now().UTC().Format(time.RFC3339))

			// Scrittura sul topic pulito
			if err := writeToKafka(ptrData, writer); err != nil {
				log.Printf("Errore invio messaggio su %s: %v", writerTopic, err)
			}
		}
	}()
}
