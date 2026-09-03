package main

import (
	"analyzer-service/package/analyzer"
	"analyzer-service/package/storage"
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	model "analyzer-service/package/config"
	pkgKafka "analyzer-service/package/kafka"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// Definizione Mappa[key: SensorID, Value: Channel]
var sensorMap sync.Map

// Definizione Mappa[key: int (Partizione), Value: Message] --> Serve per la tolleranza ai guasti per conto di Kafka
var lastMessagePerPartition sync.Map

// Definizione Mappa[key: SensorID, Value: Stato Sensore] --> Mappa temporanea contenente gli stati ripristinati
var recoveredStates map[string]model.SensorState

var CheckpointTriggerChan = make(chan struct{}, 1)

func RequestCheckpoint() {
	select {
	case CheckpointTriggerChan <- struct{}{}:
	default:
		// Se c'è già una richiesta in coda, non serve accumularne altre
	}
}

func main() {
	//	-----	1°Passo:	INIZIALIZZAZIONE DELLE CONNESIONI	-----
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9096"
	}

	//		DEFINIZIONE TOPIC DEI DATI PROCESSATI
	topic := "processed-data-topic"
	processedData := pkgKafka.NewWriterKafka(broker, topic)

	reader := pkgKafka.NewKafkaConsumer(broker)
	defer func() {
		err := reader.Close()
		if err != nil {
			log.Printf("Errore chiusura Kafka reader: %v", err)
		}
	}()

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//	-----	2°Passo:	FASE DI RECOVERY DA REDIS (GESTIONE EVENTUALI GUASTI DURANTE L'ESECUZIONE)	-----
	var errRecovery error
	recoveredStates, errRecovery = storage.LoadSnapshot(ctx, conn)
	if errRecovery != nil {
		log.Printf("[RECOVERY WARNING] Impossibile caricare snapshot da Redis: %v. Si riparte da zero.", errRecovery)
		recoveredStates = make(map[string]model.SensorState)
	} else {
		log.Printf("[RECOVERY] Ripristinati gli stati per %d sensori da Redis", len(recoveredStates))
	}

	//	-----	3°Passo:	GESTIONE SHUTDOWN	-----
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Ricevuto messaggio di arresto, avvio shutdown...")
		cancel()
	}()

	//	-----	4°Passo:	AVVIO GOROUTNE PER LA GESTIONE DEL CHECKPOINT DELLE FINESTRE	-----
	go startCheckpointTicker(ctx, reader, conn)

	log.Println("Analyzer Service attivo sul topic 'data-topic-cleaned'")

	//	-----	5°Passo:	AVVIO GOROUTINE PER LA GESTIONE DEI SEGNALI DA REDIS PUB/SUB. GESTIONE STATO STOPPED DEI SENSORI	-----
	go performStoppedStateSensor(ctx, conn)

	for {
		// Usiamo FetchMessage per gestire manualmente il commit
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("Errore lettura Kafka: %v", err)
			continue
		}

		//	Salviamo l'ultimo messaggio visto la specifica Partizione del Topic Kafka
		lastMessagePerPartition.Store(msg.Partition, msg)

		var t model.GenericTelemetry
		if err := json.Unmarshal(msg.Value, &t); err != nil {
			log.Printf("Errore unmarshal (messaggio scartato): %v", err)
			continue
		}

		parsedTime, err := time.Parse(time.RFC3339, t.Timestamp)
		if err != nil {
			// Fallback in caso di timestamp vuoto o malformato
			parsedTime = time.Now().UTC()
		}

		var item model.Item
		var valore float64

		if t.TemperatureCelsius != 0 {
			valore = t.TemperatureCelsius
		} else if t.PressureBar != 0 {
			valore = t.PressureBar
		} else {
			continue
		}

		item = model.Item{
			SensorID:  t.SensorID,
			MachineID: t.MachineID,
			Timestamp: parsedTime,
			Value:     valore,
		}

		// Inoltra l'item al worker dedicato del sensore
		AddNItem(ctx, item, processedData, conn)

	}
}

func AddNItem(ctx context.Context, item model.Item, writer *kafka.Writer, conn *redis.Client) {
	//	Definizione dei Canali
	channels := &model.SensorChannels{
		InputChannel:       make(chan model.Item, 500),
		InvalidDataChannel: make(chan string, 100),
	}

	val, loader := sensorMap.LoadOrStore(item.SensorID, channels)
	channel := val.(*model.SensorChannels)

	if !loader {
		var initialState *model.SensorState
		if state, ok := recoveredStates[item.SensorID]; ok {
			initialState = &state
			delete(recoveredStates, item.SensorID)
		}

		go analyzer.SensorWorker(ctx, item.SensorID, channel, initialState, writer, conn, RequestCheckpoint)
	}
	channel.InputChannel <- item
}

func startCheckpointTicker(ctx context.Context, reader *kafka.Reader, conn *redis.Client) {
	ticker := time.NewTicker(30 * time.Second) //	Snapshot ogni 30 secondi
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			performCheckpoint(shutdownCtx, reader, conn)
			cancel()
			return

		case <-ticker.C:
			chkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			performCheckpoint(chkCtx, reader, conn)
			cancel()

		case <-CheckpointTriggerChan: // <-- Checkpoint su richiesta di invalidazione
			chkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			performCheckpoint(chkCtx, reader, conn)
			cancel()
		}
	}
}

func performCheckpoint(ctx context.Context, reader *kafka.Reader, conn *redis.Client) {
	// 	Cattura lo Snapshot in RAM
	snapshot := analyzer.CaptureWindowSnapshot()

	// 	Salvataggio su Redis
	if err := storage.SaveSnapshot(ctx, snapshot, conn); err != nil {
		log.Printf("[CHECKPOINT ERROR] Fallito salvataggio snapshot su Redis: %v. Annullamento commit Kafka.", err)
		return // SE REDIS FALLISCE, NON ESEGUO COMMIT SU KAFKA
	}

	// 	Lettura ultimi messaggi per partizione
	var messages []kafka.Message
	lastMessagePerPartition.Range(func(key, value any) bool {
		messages = append(messages, value.(kafka.Message))
		return true
	})

	// 	Commit atomico degli offset su Kafka
	if len(messages) > 0 {
		if err := reader.CommitMessages(ctx, messages...); err != nil {
			log.Printf("[CHECKPOINT ERROR] Errore commit offset su Kafka: %v", err)
		} else {
			log.Printf("[CHECKPOINT OK] Snapshot salvato su Redis e offset committati per %d partizioni", len(messages))
		}
	}
}

func performStoppedStateSensor(ctx context.Context, conn *redis.Client) {
	//	Sottoscrizione al topic Redis sensor:state-events
	pubSub := conn.Subscribe(ctx, `sensor:state-events`)
	defer func(pubSub *redis.PubSub) {
		err := pubSub.Close()
		if err != nil {
			log.Printf("Errore chiusura connessione Redis: %v", err)
		}
	}(pubSub)

	ch := pubSub.Channel()
	log.Printf("[DISPATCHER] In Ascolto sul topic Redis...")

	for msg := range ch {
		processEvent(msg.Payload)
	}
}

func processEvent(msg string) {
	parts := strings.Split(msg, ":")
	if len(parts) != 2 {
		return
	}

	sensorID := parts[0]
	state := parts[1]

	//	Troviamo il canale associato al sensore
	if val, ok := sensorMap.Load(sensorID); ok {
		channels := val.(*model.SensorChannels)

		//	Invio del Messaggio
		select {
		case channels.InvalidDataChannel <- state:
			log.Printf("[DISPATCHER] Notificato stato '%s' al worker %s", state, sensorID)
		default:
			log.Printf("[WARNING] Canale invalidazione pieno per %s, messaggio scartato", sensorID)
		}
	}
}
