package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	model "progettoSDCC/events-generator/package/config"
	pkgKafka "progettoSDCC/events-generator/package/kafka"
	"progettoSDCC/events-generator/package/sensor"
	"progettoSDCC/events-generator/package/storage"

	"github.com/redis/go-redis/v9"
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

	//	Inizializzazione Connessione DB Redis
	parametersRedis := model.RedisParameter{
		Address:   os.Getenv("REDIS_ADDRESS"),
		Password:  os.Getenv("REDIS_PASSWORD"),
		DefaultDB: 0,
	}

	conn, err := storage.NewRedisWriter(parametersRedis)
	if err != nil {
		log.Fatal(err)
	}
	defer func(conn *redis.Client) {
		err := conn.Close()
		if err != nil {
			log.Printf("Errore chiusura connessione Redis: %v", err)
		}
	}(conn)

	// Avvio Consumer di emergenza
	pkgKafka.StartEmergencyConsumer(ctx, broker, signalTopic, "emergency-group")

	stopChan := make(chan struct{})

	// Avvio Sensori
	baseSensors := []model.SensorConfig{
		{
			// Identificativi e impostazioni del Worker
			SensorID:         "SN7F9A2K4L1X9W3",
			Type:             "TemperatureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         92.0,
			Variance:         0.25,
			Interval:         1 * time.Second,

			// Soglie operative per il Decisore
			SogliaMinima:  85.0, // Tolleranza inferiore prima del blocco (7°C sotto la media)
			SogliaMassima: 97.0, // Tolleranza superiore prima del blocco (5°C sopra la media)
			MaxStdDev:     1.50, // Soglia instabilità (circa 3x la deviazione standard nominale di 0.50)
			MaxDrift:      0.02, // Soglia deriva: riscaldamento/raffreddamento max di 0.02°C al secondo (1.2°C al minuto)
		},
		{
			// Identificativi e impostazioni del Worker
			SensorID:         "710495823110456",
			Type:             "PressureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         3.5,
			Variance:         0.04,
			Interval:         1 * time.Second,

			// Soglie operative per il Decisore
			SogliaMinima:  2.5,  // Pressione min (bar): sotto 2.5 bar rischia la cavitazione o perdita di carico
			SogliaMassima: 5.0,  // Pressione max (bar): sopra 5.0 bar c'è rischio sovrapressione/danno alle condotte
			MaxStdDev:     0.60, // Soglia instabilità: ~3x la deviazione standard nominale (0.20 bar), cattura colpi d'ariete della pompa
			MaxDrift:      0.01, // Soglia deriva: variazione max 0.01 bar/s (0.6 bar/minuto), indica perdite del circuito graduali
		},
		//{SensorID: "B82KD91Z6X64PLQ", Type: "TemperatureSensor", MachineToControl: "freezer_01", BaseMean: -18.0, Variance: 0.50, Interval: 2 * time.Second},
		//{SensorID: "B82GG91Z6X86PLP", Type: "TemperatureSensor", MachineToControl: "homogenizer_01", BaseMean: 65.0, Variance: 0.10, Interval: 1 * time.Second},
	}

	for _, cfg := range baseSensors {
		switch cfg.Type {
		case "TemperatureSensor":
			sensor.StartSensor(ctx, cfg, stopChan, temperatureWriter, conn)
		case "PressureSensor":
			sensor.StartSensor(ctx, cfg, stopChan, pressureWriter, conn)
		}
	}

	dashServerWriter := model.NewDashboardServer(temperatureWriter, pressureWriter, signalWriter, conn)

	sensor.StartDashboardServer(ctx, "8081", stopChan, dashServerWriter)

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
