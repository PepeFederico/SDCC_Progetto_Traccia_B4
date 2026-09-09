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

			SogliaMinima:  85.0, // Sotto gli 85°C il prodotto non viene sterilizzato correttamente
			SogliaMassima: 97.0, // Sopra i 97°C rischia di bruciare il prodotto
			MaxStdDev:     1.50, // 3x std dev nominale
			MaxDrift:      0.02, // 0.02 °C/s (1.2 °C/min)
		},
		{
			// Sensore Pressione Pompa Pastorizzatore
			SensorID:         "710495823110456",
			Type:             "PressureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         3.5,
			Variance:         0.04, // std dev nominale = 0.20 bar
			Interval:         1 * time.Second,

			SogliaMinima:  2.5,  // Pericolo cavitazione sotto 2.5 bar
			SogliaMassima: 5.0,  // Pericolo sovrappressione sopra 5.0 bar
			MaxStdDev:     0.60, // 3x std dev nominale (rileva colpi d'ariete)
			MaxDrift:      0.01, // 0.01 bar/s (per perdite di carico)
		},

		// =========================================================================
		// MACCHINARIO 2: homogenizer_01 (Omogeneizzatore ad alta pressione)
		// =========================================================================
		{
			// Sensore Alta Pressione Omogeneizzazione
			SensorID:         "HOM-PRS-9921834",
			Type:             "PressureSensor",
			MachineToControl: "homogenizer_01",
			BaseMean:         180.0, // Alta pressione tipica in bar
			Variance:         4.00,  // std dev nominale = 2.0 bar
			Interval:         1 * time.Second,

			SogliaMinima:  150.0, // Sotto 150 bar la miscela non viene omogeneizzata
			SogliaMassima: 210.0, // Sopra 210 bar scatta la valvola di sicurezza
			MaxStdDev:     6.00,  // Rileva fluttuazioni anomale nel piattello valvole
			MaxDrift:      0.10,  // Deriva max 0.1 bar/s
		},
		{
			// Sensore Temperatura Fluido Omogeneizzatore
			SensorID:         "B82GG91Z6X86PLP",
			Type:             "TemperatureSensor",
			MachineToControl: "homogenizer_01",
			BaseMean:         65.0,
			Variance:         0.16, // std dev nominale = 0.40 °C
			Interval:         1 * time.Second,

			SogliaMinima:  55.0, // Temperatura troppo bassa altera la viscosità
			SogliaMassima: 75.0, // Surriscaldamento per attrito meccanico
			MaxStdDev:     1.20, // Tolleranza instabilità termica
			MaxDrift:      0.03, // Riscaldamento rapido anomalo
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
			Variance:         0.36, // std dev nominale = 0.60 °C
			Interval:         2 * time.Second,

			SogliaMinima:  -30.0, // Rischio congelamento eccessivo/spreco energetico
			SogliaMassima: -15.0, // Sbrinamento incontrollato o porta aperta
			MaxStdDev:     1.80,  // Oscillazione dovuta ai cicli di sbrinamento
			MaxDrift:      0.015, // Salita termica controllata
		},
		{
			// Sensore Pressione Gas Refrigerante (Freon/CO2)
			SensorID:         "FRZ-PRS-1029384",
			Type:             "PressureSensor",
			MachineToControl: "freezer_01",
			BaseMean:         12.5,
			Variance:         0.09, // std dev nominale = 0.30 bar
			Interval:         2 * time.Second,

			SogliaMinima:  9.0,  // Rischio perdita di gas refrigerante
			SogliaMassima: 16.0, // Blocco compressore per alta pressione
			MaxStdDev:     0.90, // Picchi di pressione del compressore
			MaxDrift:      0.02, // Perte improvvise o intasamento filtro
		},

		// =========================================================================
		// MACCHINARIO 4: fermenter_01 (Fermentatore / Bioreattore)
		// =========================================================================
		{
			// Sensore Temperatura Fermentazione
			SensorID:         "FRM-TMP-4451209",
			Type:             "TemperatureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         37.0, // Temperatura ideale per colture batteriche/lieviti
			Variance:         0.04, // std dev nominale = 0.20 °C (richiede molta stabilità)
			Interval:         1 * time.Second,

			SogliaMinima:  32.0,  // Morte/stasi dei lieviti per freddo
			SogliaMassima: 42.0,  // Morte termica della coltura
			MaxStdDev:     0.60,  // Alta sensibilità all'instabilità
			MaxDrift:      0.005, // Deriva molto lenta (max 0.3°C al minuto)
		},
		{
			// Sensore Pressione Interna Serbatoio Fermentatore
			SensorID:         "FRM-PRS-8812301",
			Type:             "PressureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         1.8,  // Pressione fissa di sovrappressione CO2 (bar)
			Variance:         0.01, // std dev nominale = 0.10 bar
			Interval:         1 * time.Second,

			SogliaMinima:  1.0,   // Pressione atmosferica = perdita tenuta stagna o contaminazione
			SogliaMassima: 2.8,   // Rischio esplosione/rottura serbatoio per accumulo CO2
			MaxStdDev:     0.30,  // Rileva blocco della valvola di sfogo
			MaxDrift:      0.008, // Deriva da fermentazione vigorosa
		},
		{
			// Sensore di Riserva / Monitoraggio Giacca di Raffreddamento Fermentatore
			SensorID:         "FRM-TMP-9941122",
			Type:             "TemperatureSensor",
			MachineToControl: "fermenter_01",
			BaseMean:         15.0, // Fluido refrigerante nell'intercapedine
			Variance:         0.25, // std dev nominale = 0.50 °C
			Interval:         2 * time.Second,

			SogliaMinima:  5.0,  // Rischio shock termico
			SogliaMassima: 25.0, // Fluido troppo caldo, non raffredda più
			MaxStdDev:     1.50,
			MaxDrift:      0.02,
		},
		{
			// Sensore Pressione Ingresso Acqua di Processo / Lavaggio
			SensorID:         "PST-PRS-3004911",
			Type:             "PressureSensor",
			MachineToControl: "pastorizer_01",
			BaseMean:         4.2,
			Variance:         0.09, // std dev nominale = 0.30 bar
			Interval:         1 * time.Second,

			SogliaMinima:  2.8, // Mancanza acqua di rete
			SogliaMassima: 6.0, // Picco di rete acquedotto
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
