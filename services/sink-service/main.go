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

		// 1. Ferma le letture da Kafka cancellando il contesto del reader
		cancel()

		// 2. Ferma in modo pulito il server gRPC
		grpcServer.GracefulStop()
	}()

	log.Println("[Sink-Service] Attivo e in ascolto su Kafka 'processed-data-topic'...")

	// --- Loop Principale Consumer Kafka ---
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancellato: arresto controllato
				break
			}
			log.Printf("[Kafka Error] Errore lettura: %v", err)
			continue
		}

		// Isolamento dell'iterazione in una closure per la gestione pulita di defer e commitCtx
		func() {
			commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer commitCancel()

			var event storage.MessageStreamEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("[Sink Error] Errore unmarshal MessageStreamEvent (scarto): %v", err)
				_ = reader.CommitMessages(commitCtx, msg)
				return
			}

			// -----------------------------------------------------------------
			// CASO B: Gestione del DATO SENSORIALE (DATA)
			// -----------------------------------------------------------------
			if event.Type == "LATENCY_MARKER" && event.Marker != nil {
				nowNano := time.Now().UnixNano()
				totalLatencyMs := float64(nowNano-event.Marker.IngressTimestampNano) / 1e6

				log.Printf("[LATENCY E2E] Latenza Totale Pipeline: %.2f ms", totalLatencyMs)

				if err := reader.CommitMessages(commitCtx, msg); err != nil {
					log.Printf("[Sink Error] Errore commit offset marker: %v", err)
				}
				return
			}

			// -----------------------------------------------------------------
			// CASO B: Gestione del DATO SENSORIALE (DATA)
			// -----------------------------------------------------------------
			if event.Type == "DATA" && len(event.Payload) > 0 {
				var t storage.ProcessedData
				if err := json.Unmarshal(event.Payload, &t); err != nil {
					log.Printf("[Sink Error] Errore unmarshal ProcessedData (scarto): %v", err)
					_ = reader.CommitMessages(commitCtx, msg)
					return
				}

				sensorID := t.SensorID
				if sensorID == "" {
					sensorID = string(msg.Key)
				}

				parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
				if err != nil {
					parsedTime = time.Now().UTC()
				}

				payload := storage.DataPoint{
					SensorID:     sensorID,
					MachineID:    t.MachineID,
					Minimo:       t.Minimo,
					Media:        t.Media,
					Massimo:      t.Massimo,
					StdDev:       t.StdDev,
					RateOfChange: t.RateOfChange,
					Timestamp:    parsedTime,
				}

				// Scrittura del dato nel buffer InfluxDB
				client.WritePoints(&payload)

				// Commit dell'offset su Kafka
				if err := reader.CommitMessages(commitCtx, msg); err != nil {
					log.Printf("[Sink Error] Errore commit offset data: %v", err)
				} else {
					log.Printf("[DEBUG] Scritto punto per macchinario: %s, sensore: %s", payload.MachineID, payload.SensorID)
				}
				return
			}

			// Commit di fallback per eventuali eventi non riconosciuti
			_ = reader.CommitMessages(commitCtx, msg)
		}()
	}

	// --- Operazioni di Chiusura e Cleanup ---
	log.Println("[Sink-Service] Chiusura Kafka Consumer...")
	if err := reader.Close(); err != nil {
		log.Printf("[Kafka Error] Errore chiusura reader: %v", err)
	}

	log.Println("[Sink-Service] Esecuzione Flush e chiusura InfluxDB Client...")
	client.Close() // Flush del buffer in memoria e chiusura della connessione

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
