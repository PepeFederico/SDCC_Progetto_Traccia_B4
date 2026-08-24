package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	pkgKafka "write-service/package/kafka"
	"write-service/package/storage"
)

type GenericTelemetry struct {
	MessageID          string  `json:"messageId"`
	SensorID           string  `json:"sensorId"`
	MachineID          string  `json:"machineId"`
	Timestamp          string  `json:"timestamp"`
	TemperatureCelsius float64 `json:"temperature_celsius,omitempty"`
	PressureBar        float64 `json:"pressure_bar,omitempty"`
}

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	influxURL := os.Getenv("INFLUXDB_URL")
	if influxURL == "" {
		influxURL = "http://influxdb:8086"
	}

	influxParameter := storage.InfluxParameter{
		URl:    influxURL,
		Token:  os.Getenv("INFLUXDB_TOKEN"),
		Org:    os.Getenv("INFLUXDB_ORGANIZATION"),
		Bucket: os.Getenv("INFLUXDB_BUCKET"),
	}

	writer := storage.NewInfluxWriter(influxParameter)
	defer writer.Close()

	reader := pkgKafka.NewKafkaConsumer(broker)
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Errore chiusura Kafka reader: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Gestione Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Ricevuto segnale di arresto, avvio shutdown...")
		cancel()
	}()

	log.Println("Writer Service attivo su topic 'data-topic-cleaned'...")

	for {
		// Usiamo FetchMessage per gestire manualmente il commit
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Errore lettura Kafka: %v", err)
			continue
		}

		var t GenericTelemetry
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("Errore unmarshal (messaggio scartato): %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		var metricType string
		var val float64
		if t.TemperatureCelsius != 0 {
			metricType = "temperature"
			val = t.TemperatureCelsius
		} else if t.PressureBar != 0 {
			metricType = "pressure"
			val = t.PressureBar
		}

		parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
		if err != nil {
			// Fallback in caso di timestamp vuoto o malformato
			parsedTime = time.Now().UTC()
		}

		// Scrittura asincrona su InfluxDB
		writer.WritePoints(t.MachineID, t.SensorID, metricType, val, parsedTime)

		// Commit manuale dell'offset dopo l'elaborazione
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Errore commit offset Kafka: %v", err)
		}

		log.Printf("[DEBUG]: scritto messaggio %s\n", msg.Value)
	}

	log.Println("Writer Service arrestato correttamente.")

}
