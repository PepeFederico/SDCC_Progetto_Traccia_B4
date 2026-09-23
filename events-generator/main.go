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

// InsertNewSensor viene invocato dall'API Gateway via gRPC per l'inserimento di un nuovo sensore
func (server *GeneratorServer) InsertNewSensor(_ context.Context, req *generatorpb.Sensor) (*generatorpb.Response, error) {
	log.Printf("[Generatore] Ricevuto comando gRPC per nuovo sensore: ID=%s, Tipo=%s", req.GetSensorId(), req.GetType())

	// Mappa il messaggio Protobuf nella struct interna del generatore (model.SensorConfig)
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

	// Avvia dinamicamente il worker usando i writer già inizializzati nel main
	switch cfg.Type {
	case "TemperatureSensor":
		sensor.StartSensor(server.ctx, cfg, server.stopChan, server.temperatureWriter, server.redisClient)
	case "PressureSensor":
		sensor.StartSensor(server.ctx, cfg, server.stopChan, server.pressureWriter, server.redisClient)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "Tipo sensore non supportato: %s", cfg.Type)
	}

	return &generatorpb.Response{
		Done:    true,
		Message: fmt.Sprintf("Sensore %s avviato con successo tramite gRPC", req.GetSensorId()),
	}, nil
}

// LoadSensorType viene invocato dall'API Gateway via gRPC per caricare le tipologie di sensori attualmente attivi
func (server *GeneratorServer) LoadSensorType(_ context.Context, _ *generatorpb.LoadSensorTypeRequest) (*generatorpb.SensorType, error) {

	types := []string{"TemperatureSensor", "PressureSensor"}
	return &generatorpb.SensorType{
		Type: types,
	}, nil
}

// RetriveActiveSensor viene invocato dall'API Gateway via gRPC per caricare i sensori attualmente attivi
func (server *GeneratorServer) RetriveActiveSensor(_ context.Context, _ *generatorpb.GetActiveSensorRequest) (*generatorpb.ActiveSensor, error) {

	activeSensor := sensor.GetActiveSensors()
	return &generatorpb.ActiveSensor{
		Sensor: activeSensor,
	}, nil
}

// ChangeMode viene invocato dall'API Gateway via gRPC per trigger al sensore specificato
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
	latencyWriter := pkgKafka.NewWriterLatencyMarker(broker)
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

	// Avvio Consumer per gestione messaggi di arresto preventivi
	pkgKafka.StartEmergencyConsumer(ctx, broker, signalTopic, "emergency-group")

	stopChan := make(chan struct{})

	// Avvio Sensori
	baseSensors := []model.SensorConfig{
		// =========================================================================
		// MACCHINARIO 1: pastorizer_01 (Pastorizzatore)
		// =========================================================================
		{
			// Sensore Temperatura Pastorizzazione
			SensorID:         "SN7F9A2K4L1X9W3",
			Type:             "TemperatureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         92.0,
			Variance:         0.25, // std dev nominale = 0.50 °C
			Interval:         1 * time.Second,

			SogliaMinima:  85.0,
			SogliaMassima: 97.0,
			MaxStdDev:     1.50,
			MaxDrift:      0.02,
		},
		{
			// Sensore Pressione Pompa Pastorizzatore
			SensorID:         "710495823110456",
			Type:             "PressureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         3.5,
			Variance:         0.04,
			Interval:         1 * time.Second,

			SogliaMinima:  2.5,
			SogliaMassima: 5.0,
			MaxStdDev:     0.60,
			MaxDrift:      0.01,
		},

		// =========================================================================
		// MACCHINARIO 2: homogenizer_01 (Omogeneizzatore ad alta pressione)
		// =========================================================================
		{
			// Sensore Alta Pressione Omogeneizzazione
			SensorID:         "HOM-PRS-9921834",
			Type:             "PressureSensor",
			MachineToControl: "homogenizer_01",
			BaseMean:         180.0,
			Variance:         4.00,
			Interval:         1 * time.Second,

			SogliaMinima:  150.0,
			SogliaMassima: 210.0,
			MaxStdDev:     6.00,
			MaxDrift:      0.10,
		},
		{
			// Sensore Temperatura Fluido Omogeneizzatore
			SensorID:         "B82GG91Z6X86PLP",
			Type:             "TemperatureSensor",
			MachineToControl: "homogenizer_01",
			BaseMean:         65.0,
			Variance:         0.16,
			Interval:         1 * time.Second,

			SogliaMinima:  55.0,
			SogliaMassima: 75.0,
			MaxStdDev:     1.20,
			MaxDrift:      0.03,
		},

		// =========================================================================
		// MACCHINARIO 3: freezer_01 (Tunnel di Congelamento / Cella Frigo)
		// =========================================================================
		{
			// Sensore Temperatura Cella Frigo
			SensorID:         "B82KD91Z6X64PLQ",
			Type:             "TemperatureSensor",
			MachineToControl: "freezer_01",
			BaseMean:         -22.0,
			Variance:         0.36,
			Interval:         2 * time.Second,

			SogliaMinima:  -30.0,
			SogliaMassima: -15.0,
			MaxStdDev:     1.80,
			MaxDrift:      0.015,
		},
		{
			// Sensore Pressione Gas Refrigerante (Freon/CO2)
			SensorID:         "FRZ-PRS-1029384",
			Type:             "PressureSensor",
			MachineToControl: "freezer_01",
			BaseMean:         12.5,
			Variance:         0.09,
			Interval:         2 * time.Second,

			SogliaMinima:  9.0,
			SogliaMassima: 16.0,
			MaxStdDev:     0.90,
			MaxDrift:      0.02,
		},

		// =========================================================================
		// MACCHINARIO 4: fermenter_01 (Fermentatore)
		// =========================================================================
		{
			// Sensore Temperatura Fermentazione
			SensorID:         "FRM-TMP-4451209",
			Type:             "TemperatureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         37.0,
			Variance:         0.04,
			Interval:         1 * time.Second,

			SogliaMinima:  32.0,
			SogliaMassima: 42.0,
			MaxStdDev:     0.60,
			MaxDrift:      0.005,
		},
		{
			// Sensore Pressione Interna Serbatoio Fermentatore
			SensorID:         "FRM-PRS-8812301",
			Type:             "PressureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         1.8,
			Variance:         0.01,
			Interval:         1 * time.Second,

			SogliaMinima:  1.0,
			SogliaMassima: 2.8,
			MaxStdDev:     0.30,
			MaxDrift:      0.008,
		},
		{
			// Sensore di Riserva / Monitoraggio Giacca di Raffreddamento Fermentatore
			SensorID:         "FRM-TMP-9941122",
			Type:             "TemperatureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         15.0,
			Variance:         0.25,
			Interval:         2 * time.Second,

			SogliaMinima:  5.0,
			SogliaMassima: 25.0,
			MaxStdDev:     1.50,
			MaxDrift:      0.02,
		},
		{
			// Sensore Pressione Ingresso Acqua di Processo / Lavaggio
			SensorID:         "PST-PRS-3004911",
			Type:             "PressureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         4.2,
			Variance:         0.09,
			Interval:         1 * time.Second,

			SogliaMinima:  2.8,
			SogliaMassima: 6.0,
			MaxStdDev:     0.90,
			MaxDrift:      0.015,
		},
	}

	for _, cfg := range baseSensors {
		switch cfg.Type {
		case "TemperatureSensor":
			sensor.StartSensor(ctx, cfg, stopChan, temperatureWriter, conn)
		case "PressureSensor":
			sensor.StartSensor(ctx, cfg, stopChan, pressureWriter, conn)
		}
	}

	// Avvio dello emitter (es. invio di un marker ogni 500 ms)
	sensor.StartLatencyMarkerEmitter(ctx, latencyWriter, topicsKafka, 500*time.Millisecond)

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
	_ = latencyWriter.Close()

	fmt.Println("Generatore di eventi arrestato correttamente.")
}
