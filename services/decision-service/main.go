package main

import (
	"context"
	pkgKafka "decision-service/package/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"

	metrics "progettoSDCC/prometheus"

	model "decision-service/package/config"
	"decision-service/package/storage"

	"github.com/redis/go-redis/v9"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	redisAddr := os.Getenv("REDIS_ADDRESS")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379" // Fallback standard
	}

	// Definizione Topic in Lettura/Scrittura
	readerTopic := "processed-data-topic"
	writerTopic := "signals-topic"

	// Inizializzazione Connessione DB Redis
	parametersRedis := model.RedisParameter{
		Address:   redisAddr,
		Password:  os.Getenv("REDIS_PASSWORD"),
		DefaultDB: 0,
	}

	//	Initialization e avvio servizio Prometheus
	metriche := metrics.NewMetricsFactory("decision_service")
	metrics.StartMetricsServer(":2112")

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pkgKafka.StartDecisionConsumer(ctx, broker, readerTopic, writerTopic, "decision-consumer", conn, metriche)

	// Shutdown pulito
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	cancel()

}
