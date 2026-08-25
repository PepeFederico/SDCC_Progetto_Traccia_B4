package main

import (
	"analyzer-service/package/analyzer"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	model "analyzer-service/package/config"
	pkgKafka "analyzer-service/package/kafka"
)

// Definizione Mappa[key: SensorID, Value: Channel]
var sensorMap sync.Map

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	reader := pkgKafka.NewKafkaConsumer(broker)
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("Errore chiusura Kafka reader: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//	Gestione shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Ricevuto messaggio di arresto, avvio shutdown...")
		cancel()
	}()

	log.Println("Analyzer Service attivo sul topic 'data-topic-cleaned'")

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

		var t model.GenericTelemetry
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

		var item model.Item

		if t.TemperatureCelsius != 0 {
			item = model.Item{
				SensorID:  t.SensorID,
				MachineID: t.MachineID,
				Timestamp: parsedTime,
				Value:     t.TemperatureCelsius,
			}

		} else if t.PressureBar != 0 {
			item = model.Item{
				SensorID:  t.SensorID,
				MachineID: t.MachineID,
				Timestamp: parsedTime,
				Value:     t.PressureBar,
			}

		} else {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		// Inoltra l'item al worker dedicato del sensore
		AddNItem(item)

		// Commit del messaggio Kafka dopo l'inoltro
		_ = reader.CommitMessages(ctx, msg)

		/*
			TODO:
				1)	Definire bene come gestire gli eventuali fallimenti
				2)	Servizio Senza Stato (Demandato al Servizio Decisore)
		*/
	}
}

func AddNItem(item model.Item) {
	val, loader := sensorMap.LoadOrStore(item.SensorID, make(chan model.Item, 500))
	channel := val.(chan model.Item)

	if !loader {
		go analyzer.SensorWorker(channel)
	}
	channel <- item
}
