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
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "127.0.0.1:9094" // Fallback per l'esecuzione in locale senza Docker
	}
	topicsKafka := []string{
		"temperature-topic-sensor",
		"pressure-topic-sensor",
	}
	signalTopic := "signals-topic"

	// Inizializzazione Client Kafka
	temperatureWriter := pkgKafka.NewWriter(broker, topicsKafka[0])
	pressureWriter := pkgKafka.NewWriter(broker, topicsKafka[1])
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
		//{SensorID: "B82KD91Z6X64PLQ", Type: "TemperatureSensor", MachineToControl: "freezer_01", BaseMean: -18.0, Variance: 0.50, Interval: 2 * time.Second},
		//{SensorID: "B82GG91Z6X86PLP", Type: "TemperatureSensor", MachineToControl: "homogenizer_01", BaseMean: 65.0, Variance: 0.10, Interval: 1 * time.Second},
	}

	for _, cfg := range baseSensors {
		switch cfg.Type {
		case "TemperatureSensor":
			sensor.StartSensor(cfg, stopChan, temperatureWriter)
		case "PressureSensor":
			sensor.StartSensor(cfg, stopChan, pressureWriter)
		}
	}

	dashServerWriter := config.NewDashboardServer(temperatureWriter, pressureWriter, signalWriter)

	sensor.StartDashboardServer("8081", stopChan, dashServerWriter)

	//time.Sleep(20 * time.Second)

	// Simulazione invio segnale di stop via Kafka
	//pkgKafka.SimulationReceiveMessage(signalWriter, "SN7F9A2K4L1X9W3")

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nArresto applicazione...")
	close(stopChan) // Segnala a Sensori e Dashboard HTTP di fermarsi

	// Dà il tempo alle goroutine di uscire dai loop prima di chiudere i socket TCP
	time.Sleep(200 * time.Millisecond)

	_ = temperatureWriter.Close()
	_ = pressureWriter.Close()
	_ = signalWriter.Close()

	fmt.Println("Generatore di eventi arrestato correttamente.")
}
