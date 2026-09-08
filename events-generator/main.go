package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	generatorpb "progettoSDCC/proto/event-generator"
	"syscall"
	"time"

	model "progettoSDCC/events-generator/package/config"
	pkgKafka "progettoSDCC/events-generator/package/kafka"
	"progettoSDCC/events-generator/package/sensor"
	"progettoSDCC/events-generator/package/storage"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GeneratorServer struct {
	generatorpb.UnimplementedEventGeneratorServer
	temperatureWriter *kafka.Writer
	pressureWriter    *kafka.Writer
	redisClient       *redis.Client
	ctx               context.Context
	stopChan          chan struct{}
}

// InsertNewSensor viene invocato dall'API Gateway via gRPC
func (server *GeneratorServer) InsertNewSensor(_ context.Context, req *generatorpb.Sensor) (*generatorpb.Response, error) {
	log.Printf("[Generatore] Ricevuto comando gRPC per nuovo sensore: ID=%s, Tipo=%s", req.GetSensorId(), req.GetType())

	// 1. Mappa il messaggio Protobuf nella struct interna del generatore (model.SensorConfig)
	cfg := model.SensorConfig{
		SensorID:         req.GetSensorId(),
		Type:             req.GetType(),
		MachineToControl: req.GetMachineToControl(),
		BaseMean:         float64(req.GetBaseMean()),
		Variance:         float64(req.GetVariance()),
		Interval:         time.Duration(req.GetIntervalNano()),
		SogliaMinima:     float64(req.GetSogliaMinima()),
		SogliaMassima:    float64(req.GetSogliaMassima()),
		MaxStdDev:        float64(req.GetMaxStdDev()),
		MaxDrift:         float64(req.GetMaxDrift()),
	}

	// 2. Avvia dinamicamente il worker usando i writer già inizializzati nel main
	switch cfg.Type {
	case "TemperatureSensor":
		sensor.StartSensor(server.ctx, cfg, server.stopChan, server.temperatureWriter, server.redisClient)
	case "PressureSensor":
		sensor.StartSensor(server.ctx, cfg, server.stopChan, server.pressureWriter, server.redisClient)
	}

	return &generatorpb.Response{
		Done:    true,
		Message: fmt.Sprintf("Sensore %s avviato con successo tramite gRPC", req.GetSensorId()),
	}, nil
}

// LoadSensorType viene invocato dall'API Gateway via gRPC
func (server *GeneratorServer) LoadSensorType(_ context.Context, _ *generatorpb.LoadSensorTypeRequest) (*generatorpb.SensorType, error) {

	types := []string{"TemperatureSensor", "PressureSensor"}
	return &generatorpb.SensorType{
		Type: types,
	}, nil
}

// RetriveActiveSensor viene invocato dall'API Gateway via gRPC
func (server *GeneratorServer) RetriveActiveSensor(_ context.Context, _ *generatorpb.GetActiveSensorRequest) (*generatorpb.ActiveSensor, error) {

	activeSensor := sensor.GetActiveSensors()
	return &generatorpb.ActiveSensor{
		Sensor: activeSensor,
	}, nil
}

// ChangeMode viene invocato dall'API Gateway via gRPC
func (server *GeneratorServer) ChangeMode(_ context.Context, req *generatorpb.Mode) (*generatorpb.Response, error) {
	log.Printf("[Generatore] Ricevuto comando gRPC per il sensore: ID=%s", req.GetSensorId())

	cmd := model.StateCommand{
		Mode:           model.OperationalMode(req.GetMode()),
		DriftRate:      float64(req.GetValue()),
		SpikeMagnitude: float64(req.GetValue()),
		SensorID:       req.GetSensorId(),
	}

	// Invio del comando al sensore
	if success := sensor.SendControlCommand(cmd.SensorID, cmd); !success {
		// In gRPC non si usa http.Error, ma i codici di stato gRPC
		return nil, status.Errorf(codes.NotFound, "Sensore %s non trovato", cmd.SensorID)
	}

	// Risposta gRPC corretta in caso di successo
	return &generatorpb.Response{
		Done:    true,
		Message: fmt.Sprintf("Comando inviato con successo al sensore %s", cmd.SensorID),
	}, nil
}

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

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Impossibile mettersi in ascolto sulla porta gRPC 50051: %v", err)
	}

	grpcServer := grpc.NewServer()
	generatorServerInstance := &GeneratorServer{
		temperatureWriter: temperatureWriter,
		pressureWriter:    pressureWriter,
		redisClient:       conn,
		ctx:               ctx,
		stopChan:          stopChan,
	}

	generatorpb.RegisterEventGeneratorServer(grpcServer, generatorServerInstance)

	go func() {
		log.Println("[Events-Generator] Server gRPC in ascolto sulla porta :50051...")
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Errore durante l'esecuzione del server gRPC: %v", err)
		}
	}()

	// Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nArresto applicazione...")
	close(stopChan)

	grpcServer.GracefulStop()

	// Dà il tempo alle goroutine di uscire dai loop prima di chiudere i socket TCP
	time.Sleep(200 * time.Millisecond)

	_ = temperatureWriter.Close()
	_ = pressureWriter.Close()
	_ = signalWriter.Close()

	fmt.Println("Generatore di eventi arrestato correttamente.")
}
