package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"

	pkgKafka "sink-service/package/kafka"
	"sink-service/package/storage"
	"syscall"
	"time"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	influxURL := os.Getenv("INFLUXDB_URL")
	if influxURL == "" {
		influxURL = "http://localhost:8086"
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

	log.Println("Sink Service attivo su topic 'processed-data-topic'...")

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

		var t storage.ProcessedData
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("Errore unmarshal (messaggio scartato): %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
		if err != nil {
			// Fallback in caso di timestamp vuoto o malformato
			parsedTime = time.Now().UTC()
		}

		payload := storage.DataPoint{
			SensorID:     t.SensorID,
			MachineID:    t.MachineID,
			Minimo:       t.Minimo,
			Media:        t.Media,
			Massimo:      t.Massimo,
			StdDev:       t.StdDev,
			RateOfChange: t.RateOfChange,
			Timestamp:    parsedTime,
		}

		// Scrittura asincrona su InfluxDB
		writer.WritePoints(&payload)

		// Commit manuale dell'offset dopo l'elaborazione
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Errore commit offset Kafka: %v", err)
		}

		log.Printf("[DEBUG]: scritto messaggio %v\n", payload)
	}

	log.Println("Writer Service arrestato correttamente.")

}
