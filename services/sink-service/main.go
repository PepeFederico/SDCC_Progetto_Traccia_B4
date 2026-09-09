package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	sinkpb "progettoSDCC/proto/sink-service"
	pkgKafka "sink-service/package/kafka"
	"sink-service/package/storage"

	"google.golang.org/grpc"
)

type SinkServer struct {
	sinkpb.UnimplementedSinkServiceServer
	influxClient *storage.InfluxClient
}

func main() {
	// --- Configurazione Environment ---
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	influxURL := os.Getenv("INFLUXDB_URL")
	if influxURL == "" {
		influxURL = "http://localhost:8086"
	}

	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50052"
	}

	influxParameter := storage.InfluxParameter{
		URl:    influxURL,
		Token:  os.Getenv("INFLUXDB_TOKEN"),
		Org:    os.Getenv("INFLUXDB_ORGANIZATION"),
		Bucket: os.Getenv("INFLUXDB_BUCKET"),
	}

	// --- Inizializzazione Storage e Kafka ---
	client := storage.NewInfluxClient(influxParameter)
	reader := pkgKafka.NewKafkaConsumer(broker)

	ctx, cancel := context.WithCancel(context.Background())

	// --- Inizializzazione Server gRPC ---
	grpcServer := StartGRPCServer(grpcPort, client)

	// --- Gestione Signal Shutdown ---
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("[Sink-Service] Ricevuto segnale di arresto, avvio shutdown...")

		// 1. Ferma le letture da Kafka
		cancel()

		// 2. Ferma in modo pulito il server gRPC (non accetta più nuove RPC)
		grpcServer.GracefulStop()
	}()

	log.Println("[Sink-Service] Attivo e in ascolto su Kafka 'processed-data-topic'...")

	// --- Loop Principale Consumer Kafka ---
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancellato: usciamo dal loop puliti
				break
			}
			log.Printf("[Kafka Error] Errore lettura: %v", err)
			continue
		}

		var t storage.ProcessedData
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("[Kafka Error] Errore unmarshal (messaggio scartato): %v", err)
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
		if err != nil {
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

		// Scrittura asincrona in buffer
		client.WritePoints(&payload)

		// Commit offset
		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("[Kafka Error] Errore commit offset: %v", err)
		}

		log.Printf("[DEBUG] Scritto punto per macchinario: %s, sensore: %s", payload.MachineID, payload.SensorID)
	}

	// --- Operazioni di Chiusura e Cleanup ---
	log.Println("[Sink-Service] Chiusura Kafka Consumer...")
	if err := reader.Close(); err != nil {
		log.Printf("[Kafka Error] Errore chiusura reader: %v", err)
	}

	log.Println("[Sink-Service] Esecuzione Flush e chiusura InfluxDB Client...")
	client.Close() // Garantisce che tutto ciò che è in memoria venga salvato sul DB prima di uscire

	log.Println("[Sink-Service] Arrestato correttamente.")
}

func StartGRPCServer(port string, client *storage.InfluxClient) *grpc.Server {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("[gRPC Fatal] Impossibile aprire la porta %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	sinkServerInstance := &SinkServer{
		influxClient: client,
	}

	sinkpb.RegisterSinkServiceServer(grpcServer, sinkServerInstance)

	go func() {
		log.Printf("[Sink-Service] Server gRPC in ascolto sulla porta :%s...", port)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Fatalf("[gRPC Fatal] Errore esecuzione server gRPC: %v", err)
		}
	}()

	return grpcServer
}

func (s *SinkServer) GetMetricsSnapshot(ctx context.Context, _ *sinkpb.SnapshotRequest) (*sinkpb.SnapshotResponse, error) {
	metrics, err := s.influxClient.GetMetricsSnapshot(ctx)
	if err != nil {
		log.Printf("[gRPC Error] Errore recupero snapshot da InfluxDB: %v", err)
		return nil, fmt.Errorf("errore interno durante la lettura delle metriche: %w", err)
	}

	return &sinkpb.SnapshotResponse{
		Metrics: metrics,
		Message: "SUCCESS",
	}, nil
}
