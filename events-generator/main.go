package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"progettoSDCC/events-generator/package/config"
	pkgKafka "progettoSDCC/events-generator/package/kafka"
	"progettoSDCC/events-generator/package/sensor"
)

func main() {
	broker := "127.0.0.1:9094"
	telemetryTopic := "events-topic"
	signalTopic := "signals-topic"

	// Inizializzazione Client Kafka
	telemetryWriter := pkgKafka.NewWriter(broker, telemetryTopic)
	signalWriter := pkgKafka.NewWriter(broker, signalTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Avvio Consumer di emergenza
	pkgKafka.StartEmergencyConsumer(ctx, broker, signalTopic, "emergency-group")

	stopChan := make(chan struct{})

	// Avvio Sensori
	baseSensors := []config.SensorConfig{
		{SensorID: "SN7F9A2K4L1X9W3", Type: "TemperatureSensor", MachineToControl: "pastorizer_01", BaseMean: 92.0, Variance: 0.25, Interval: 1 * time.Second},
		{SensorID: "710495823110456", Type: "PressureSensor", MachineToControl: "pastorizer_01", BaseMean: 3.5, Variance: 0.04, Interval: 1 * time.Second},
		{SensorID: "B82KD91Z6X64PLQ", Type: "TemperatureSensor", MachineToControl: "freezer_01", BaseMean: -18.0, Variance: 0.50, Interval: 2 * time.Second},
		{SensorID: "B82GG91Z6X86PLP", Type: "TemperatureSensor", MachineToControl: "homogenizer_01", BaseMean: 65.0, Variance: 0.10, Interval: 1 * time.Second},
	}

	for _, cfg := range baseSensors {
		sensor.StartSensor(cfg, stopChan, telemetryWriter)
	}

	time.Sleep(3 * time.Second)

	// Simulazione invio segnale di stop via Kafka
	pkgKafka.SimulationReceiveMessage(signalWriter, "B82KD91Z6X64PLQ")

	// Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nArresto applicazione...")
	close(stopChan)
	_ = telemetryWriter.Close()
	_ = signalWriter.Close()
}
