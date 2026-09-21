package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"ingestion-service/package/config"
	pkgKafka "ingestion-service/package/kafka"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "127.0.0.1:9094"
	}

	writerTopic := "data-topic-cleaned"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consumer per TEMPERATURA
	pkgKafka.StartSensorDataConsumer[config.TemperatureReading](
		ctx,
		broker,
		"temperature-topic-sensor",
		writerTopic,
		"temperature-group",
		func(temp float64) bool {
			return temp > -100 && temp < 400 // Temperatura valida
		},
	)

	// Consumer per PRESSIONE
	pkgKafka.StartSensorDataConsumer[config.PressureReading](
		ctx,
		broker,
		"pressure-topic-sensor",
		writerTopic,
		"pressure-group",
		func(press float64) bool {
			return press >= 0 && press <= 300 // Pressione valida
		},
	)

	// Shutdown pulito
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	cancel()
}
