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
	MessageID          string   `json:"messageId"`
	SensorID           string   `json:"sensorId"`
	MachineID          string   `json:"machineId"`
	Timestamp          string   `json:"timestamp"`
	TemperatureCelsius *float64 `json:"temperature_celsius,omitempty"` //	Gestione del caso 0.0
	PressureBar        *float64 `json:"pressure_bar,omitempty"`        //	Gestione del caso 0.0
}

type MessageStreamEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"` // Presente solo se Type == "DATA"
	Marker  *LatencyMarker  `json:"marker,omitempty"`  // Presente solo se Type == "LATENCY_MARKER"
}

type LatencyMarker struct {
	IngressTimestampNano int64 `json:"ingress_ts_nano"`
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

	// Gestione Signal Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Ricevuto segnale di arresto, avvio shutdown...")
		cancel()
	}()

	log.Println("Write Service attivo su topic Kafka 'data-topic-cleaned'...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancellato: arresto pulito
				break
			}
			log.Printf("Errore lettura Kafka: %v", err)
			continue
		}

		// Closure per garantire il rilascio immediato del commitCtx a ogni iterazione
		func() {
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer commitCancel()

			// 1. Decodifica dell'involucro dell'evento
			var event MessageStreamEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Errore unmarshal MessageStreamEvent (messaggio scartato): %v", err)
				_ = reader.CommitMessages(commitCtx, msg)
				return
			}

			// --- CASO A: LATENCY_MARKER ---
			if event.Type == "LATENCY_MARKER" && event.Marker != nil {
				nowNano := time.Now().UnixNano()
				totalLatencyMs := float64(nowNano-event.Marker.IngressTimestampNano) / 1e6

				log.Printf("[LATENCY RAW-PIPELINE] Latenza Totale a InfluxDB (Raw Data): %.2f ms", totalLatencyMs)

				if err := reader.CommitMessages(commitCtx, msg); err != nil {
					log.Printf("Errore commit offset marker: %v", err)
				}
				return
			}

			// --- CASO B: DATA ---
			if event.Type == "DATA" && len(event.Payload) > 0 {
				var t GenericTelemetry
				if err := json.Unmarshal(event.Payload, &t); err != nil {
					log.Printf("Errore unmarshal GenericTelemetry (payload scartato): %v", err)
					_ = reader.CommitMessages(commitCtx, msg)
					return
				}

				// Fallback sulla Key di Kafka se SensorID nel payload JSON è vuoto
				sensorID := t.SensorID
				if sensorID == "" {
					sensorID = string(msg.Key)
				}

				// Determinazione del tipo di metrica e valore
				var metricType string
				var val float64

				if t.TemperatureCelsius != nil {
					metricType = "temperature"
					val = *t.TemperatureCelsius
				} else if t.PressureBar != nil {
					metricType = "pressure"
					val = *t.PressureBar
				} else {
					log.Printf("Messaggio ignorato: nessun valore valido per sensor %s", sensorID)
					_ = reader.CommitMessages(commitCtx, msg)
					return
				}

				parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
				if err != nil {
					parsedTime = time.Now().UTC()
				}

				// Scrittura asincrona su InfluxDB
				writer.WritePoints(t.MachineID, sensorID, metricType, val, parsedTime)

				// Commit manuale dell'offset
				if err := reader.CommitMessages(commitCtx, msg); err != nil {
					log.Printf("Errore commit offset Kafka: %v", err)
				} else {
					log.Printf("[DEBUG] Scritta metrica '%s' per macchina: %s, sensore: %s", metricType, t.MachineID, sensorID)
				}
				return
			}

			// Fallback per eventi con tipo sconosciuto
			_ = reader.CommitMessages(commitCtx, msg)
		}()
	}

	log.Println("Write Service arrestato correttamente.")
}
