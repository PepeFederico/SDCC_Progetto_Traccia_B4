package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

// Mappa globale [ID_Sensore -> Canale di controllo]
var machineRegistry sync.Map

// OperationalMode : Definisce i possibili stati del sensore
type OperationalMode int

const (
	ModeNormal OperationalMode = iota // Funzionamento operativo standard
	ModeSpike                         // Genera un singolo picco anomalo e torna Normal
	ModeDrift                         // Degrado progressivo del trend --> Simulazione anomalia
)

// SensorConfig : Definisce la configurazione iniziale di un sensore
//
//	MachineID	: Nome della macchina
//	BaseMean	: Valore medio
//	Variance	: Varianza
//	Interval	: Rate di campionamento
type SensorConfig struct {
	MachineID string
	BaseMean  float64
	Variance  float64
	Interval  time.Duration
}

// TemperatureReading : Rappresenta la lettura telemetrica inviata dal sensore
type TemperatureReading struct {
	MachineID   string  `json:"machineId"`
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature_celsius"`
}

// StateCommand : Rappresenta un messaggio di controllo/cambio stato
type StateCommand struct {
	Mode           OperationalMode
	DriftRate      float64
	SpikeMagnitude float64
}

// StartSensor : Registra ed avvia un nuovo sensore in una propria goroutine
func StartSensor(cfg SensorConfig, stopChan <-chan struct{}, kafkaWriter *kafka.Writer) chan StateCommand {
	modeChannel := make(chan StateCommand, 10)

	// Registrazione nel registro globale
	machineRegistry.Store(cfg.MachineID, modeChannel)

	// Avvio della goroutine worker
	go TemperatureSensorWorker(
		cfg.MachineID,
		cfg.BaseMean,
		cfg.Variance,
		cfg.Interval,
		stopChan,
		modeChannel,
		kafkaWriter,
	)

	return modeChannel
}

// TemperatureSensorWorker : E' il ciclo di vita concorrente della singola macchina
func TemperatureSensorWorker(machineID string, baseMean float64, variance float64, interval time.Duration, stopChan <-chan struct{}, modeChan <-chan StateCommand, kafkaWriter *kafka.Writer) {

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	stdDev := math.Sqrt(variance)

	// --- Variabili di Stato Interne --- //
	currentMode := ModeNormal
	currentMean := baseMean
	driftRate := 0.0
	spikeValue := 0.0

	fmt.Printf("[%s] Avvio monitoraggio temperatura (Media: %.2f°C, Varianza: %.2f)\n", machineID, baseMean, variance)

	for {
		select {
		case <-stopChan:
			fmt.Printf("[%s] Arresto goroutine sensore.\n", machineID)
			return

		case cmd := <-modeChan:
			currentMode = cmd.Mode
			switch cmd.Mode {
			case ModeDrift:
				driftRate = cmd.DriftRate
				fmt.Printf("[%s] CAMBIO STATO -> DRIFT (Rate: %.2f°C/ciclo)\n", machineID, driftRate)

			case ModeSpike:
				spikeValue = cmd.SpikeMagnitude
				fmt.Printf("[%s] CAMBIO STATO -> SPIKE ISOLATO (+%.2f°C)\n", machineID, spikeValue)

			default:
				currentMean = baseMean
				driftRate = 0.0
				fmt.Printf("[%s] CAMBIO STATO -> NORMALE\n", machineID)
			}

		case <-ticker.C:
			// Generazione del rumore gaussiano
			noise := stdDev * rand.NormFloat64()

			switch currentMode {
			case ModeDrift:
				currentMean += driftRate

			case ModeSpike:
				noise += spikeValue
				currentMode = ModeNormal

			default:
			}

			temp := currentMean + noise

			payload := TemperatureReading{
				MachineID:   machineID,
				Timestamp:   time.Now().UTC().Format(time.RFC3339),
				Temperature: math.Round(temp*100) / 100,
			}

			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "[%s] Errore serializzazione JSON: %v\n", machineID, err)
				continue
			}

			msg := kafka.Message{
				Key:   []byte(machineID),
				Value: jsonBytes,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = kafkaWriter.WriteMessages(ctx, msg)
			cancel()

			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "[%s] Errore info Kafka: %v\n", machineID, err)
			} else {
				fmt.Printf("[%s] Messaggio inviato a Kafka: %s\n", machineID, string(jsonBytes))
			}
		}
	}
}

// SendControlCommand : Permette di inviare un comando cercando la macchina nel registry
func SendControlCommand(machineID string, cmd StateCommand) bool {
	if ch, ok := machineRegistry.Load(machineID); ok {
		ch.(chan StateCommand) <- cmd
		return true
	}
	fmt.Printf("Macchina %s non trovata nel registro\n", machineID)
	return false
}

func main() {
	//	Inizializzazione del KafkaWriter condiviso
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9094"),
		Topic:    "events-topic",
		Balancer: &kafka.LeastBytes{},
		Async:    false,
	}

	stopChan := make(chan struct{})

	// 2. SENSORI BASE
	baseSensors := []SensorConfig{
		{MachineID: "pastorizer_01", BaseMean: 92.0, Variance: 0.25, Interval: 1 * time.Second},
		{MachineID: "freezer_01", BaseMean: -18.0, Variance: 0.50, Interval: 2 * time.Second},
		{MachineID: "homogenizer_01", BaseMean: 65.0, Variance: 0.10, Interval: 1 * time.Second},
	}

	fmt.Println("=== AVVIO SENSORI BASE ===")
	for _, cfg := range baseSensors {
		StartSensor(cfg, stopChan, kafkaWriter)
	}

	time.Sleep(3 * time.Second)

	//	AGGIUNTA DINAMICA A RUNTIME
	fmt.Println("\n=== AGGIUNTA DINAMICA SENSORE A RUNTIME ===")
	newSensorCfg := SensorConfig{
		MachineID: "pastorizer_02",
		BaseMean:  91.5,
		Variance:  0.30,
		Interval:  1 * time.Second,
	}
	StartSensor(newSensorCfg, stopChan, kafkaWriter)

	time.Sleep(3 * time.Second)

	//	INIEZIONE COMANDO TRAMITE REGISTRY
	fmt.Println("\n=== INIEZIONE SPIKE SU pastorizer_01 ===")
	SendControlCommand("pastorizer_01", StateCommand{Mode: ModeSpike, SpikeMagnitude: 25.0})

	// ------------------------------------------------------------------
	//	BLOCCO MANUALE
	// ------------------------------------------------------------------
	fmt.Println("\n Simulatore in esecuzione continua.")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan

	fmt.Println("\n=== ARRESTO MANUALE RICEVUTO: CHIUSURA IN CORSO ===")
	close(stopChan)
	time.Sleep(500 * time.Millisecond)
	if err := kafkaWriter.Close(); err != nil {
		log.Printf("Errore durante la chiusura di KafkaWriter: %v", err)
	} else {
		fmt.Println("Connessione a Kafka chiusa correttamente.")
	}

	fmt.Println("Simulatore arrestato.")
}
