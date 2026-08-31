package sensor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"time"

	"progettoSDCC/events-generator/package/config"

	"github.com/oklog/ulid/v2"
	"github.com/segmentio/kafka-go"
)

func generateULID() string {
	return ulid.Make().String()
}
func randomRange(min, max int) int {
	return min + rand.IntN(max-min+1)
}

func temperatureSensorWorker(cfg config.SensorConfig, stopChan <-chan struct{}, modeChan <-chan config.StateCommand, kafkaWriter *kafka.Writer) {
	defer machineRegistry.Delete(cfg.SensorID)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	stdDev := math.Sqrt(cfg.Variance)
	currentMode := config.ModeNormal
	currentMean := cfg.BaseMean
	driftRate := 0.0
	spikeValue := 0.0
	var payload config.TemperatureReading

	fmt.Printf("[Macchina: %s -- Sensore: %s] Avvio monitoraggio (Media: %.2f°C)\n", cfg.MachineToControl, cfg.SensorID, cfg.BaseMean)

	for {
		select {
		case <-stopChan:
			log.Printf("[%s] Arresto sensore.\n", cfg.SensorID)
			return

		case cmd := <-modeChan:
			log.Printf("[Macchina: %s -- Sensore: %s] Ricevuto comando: %s (Spike: %.2f, Drift: %.2f)\n",
				cfg.MachineToControl, cfg.SensorID, cmd.Mode, cmd.SpikeMagnitude, cmd.DriftRate)

			currentMode = cmd.Mode
			switch cmd.Mode {
			case config.ModeStop:
				log.Printf("[Macchina: %s -- Sensore: %s] Emergenza! Arresto preventivo.\n", cfg.MachineToControl, cfg.SensorID)

				// Simulazione riparazione senza bloccare lo stopChan
				timeout := time.Duration(randomRange(30, 120)) * time.Second
				select {
				case <-stopChan:
					log.Printf("[%s] Arresto sensore durante la riparazione.\n", cfg.SensorID)
					return
				case <-time.After(timeout):
					// Ripristino corretto funzionamento al termine della riparazione
					currentMode = config.ModeNormal
					currentMean = cfg.BaseMean
					driftRate = 0.0
					log.Printf("[%s] Ripristino corretto funzionamento!.\n", cfg.SensorID)
				}

			case config.ModeDrift:
				driftRate = cmd.DriftRate

			case config.ModeSpike:
				spikeValue = cmd.SpikeMagnitude

			case config.ModeNormal:
				// Reset esplicito ai valori base
				currentMean = cfg.BaseMean
				driftRate = 0.0
				spikeValue = 0.0
			}

		case <-ticker.C:
			if currentMode == config.ModeStop {
				continue
			}

			noise := stdDev * rand.NormFloat64()
			switch currentMode {
			case config.ModeDrift:
				currentMean += driftRate
			case config.ModeSpike:
				noise += spikeValue
				spikeValue = 0.0
				currentMode = config.ModeNormal
			}

			temp := currentMean + noise
			whatWeDo := rand.Float64()

			if whatWeDo < 0.03 {
				// CASO 1: Dato Corrotto
				payload = config.TemperatureReading{
					MessageID:   generateULID(),
					SensorID:    cfg.SensorID,
					MachineID:   cfg.MachineToControl,
					Timestamp:   time.Now().UTC().Format(time.RFC3339),
					Temperature: -999.99,
				}

			} else if whatWeDo < 0.08 {
				// CASO 2: Picco Fuori Scala (Outlier)
				outlierTemp := currentMean + (noise * 5)

				payload = config.TemperatureReading{
					MessageID:   generateULID(),
					SensorID:    cfg.SensorID,
					MachineID:   cfg.MachineToControl,
					Timestamp:   time.Now().UTC().Format(time.RFC3339),
					Temperature: math.Round(outlierTemp*100) / 100,
				}

			} else {
				// CASO 3: Flusso Normale
				payload = config.TemperatureReading{
					MessageID:   generateULID(),
					SensorID:    cfg.SensorID,
					MachineID:   cfg.MachineToControl,
					Timestamp:   time.Now().UTC().Format(time.RFC3339),
					Temperature: math.Round(temp*100) / 100,
				}
			}

			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				log.Printf("[%s] Errore serializzazione JSON: %v", cfg.SensorID, err)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = kafkaWriter.WriteMessages(ctx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytes})
			cancel()

			if err != nil {
				log.Printf("[%s] Errore invio Kafka: %v", cfg.SensorID, err)
			} else {
				fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
			}
		}
	}
}

func pressureSensorWorker(cfg config.SensorConfig, stopChan <-chan struct{}, modeChan <-chan config.StateCommand, kafkaWriter *kafka.Writer) {
	defer machineRegistry.Delete(cfg.SensorID)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	stdDev := math.Sqrt(cfg.Variance)
	currentMode := config.ModeNormal
	currentMean := cfg.BaseMean
	driftRate := 0.0
	spikeValue := 0.0
	var payload config.PressureReading

	fmt.Printf("[Macchina: %s -- Sensore: %s] Avvio monitoraggio (Media: %.2fBar)\n", cfg.MachineToControl, cfg.SensorID, cfg.BaseMean)

	for {
		select {
		case <-stopChan:
			log.Printf("[%s] Arresto sensore.\n", cfg.SensorID)
			return

		case cmd := <-modeChan:
			log.Printf("[Macchina: %s -- Sensore: %s] Ricevuto comando: %s (Spike: %.2f, Drift: %.2f)\n",
				cfg.MachineToControl, cfg.SensorID, cmd.Mode, cmd.SpikeMagnitude, cmd.DriftRate)
			currentMode = cmd.Mode
			switch cmd.Mode {
			case config.ModeStop:
				log.Printf("[Macchina: %s -- Sensore: %s] Emergenza! Arresto preventivo.\n", cfg.MachineToControl, cfg.SensorID)

				// Simulazione riparazione senza bloccare lo stopChan
				timeout := time.Duration(randomRange(30, 120)) * time.Second
				select {
				case <-stopChan:
					log.Printf("[%s] Arresto sensore durante la riparazione.\n", cfg.SensorID)
					return
				case <-time.After(timeout):
					// Ripristino corretto funzionamento al termine della riparazione
					currentMode = config.ModeNormal
					currentMean = cfg.BaseMean
					driftRate = 0.0
					log.Printf("[%s] Ripristino corretto funzionamento!.\n", cfg.SensorID)
				}
			case config.ModeDrift:
				driftRate = cmd.DriftRate
			case config.ModeSpike:
				spikeValue = cmd.SpikeMagnitude
			default:
				currentMean = cfg.BaseMean
				driftRate = 0.0
				spikeValue = 0.0
			}

		case <-ticker.C:
			if currentMode == config.ModeStop {
				continue
			}

			noise := stdDev * rand.NormFloat64()
			switch currentMode {
			case config.ModeDrift:
				currentMean += driftRate
			case config.ModeSpike:
				noise += spikeValue
				spikeValue = 0.0
				currentMode = config.ModeNormal
			default:

			}

			temp := currentMean + noise
			// 2. Estraiamo un singolo valore per decidere il tipo di evento
			whatWeDo := rand.Float64()

			if whatWeDo < 0.03 {
				// CASO 1 (3% delle volte): Dato Corrotto (es. NaN)
				payload = config.PressureReading{
					MessageID: generateULID(),
					SensorID:  cfg.SensorID,
					MachineID: cfg.MachineToControl,
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Pressure:  -999.99,
				}

			} else if whatWeDo < 0.08 {
				// CASO 2 (5% delle volte, cioè tra 0.03 e 0.08): Picco Fuori Scala (Outlier)
				outlierTemp := currentMean + (noise * 5) // Moltiplichiamo il rumore per generare un picco

				payload = config.PressureReading{
					MessageID: generateULID(),
					SensorID:  cfg.SensorID,
					MachineID: cfg.MachineToControl,
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Pressure:  math.Round(outlierTemp*100) / 100,
				}

			} else {
				// CASO 3 (92% delle volte): Flusso Normale
				payload = config.PressureReading{
					MessageID: generateULID(),
					SensorID:  cfg.SensorID,
					MachineID: cfg.MachineToControl,
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Pressure:  math.Round(temp*100) / 100,
				}
			}

			jsonBytes, err := json.Marshal(payload)
			if err != nil {
				log.Printf("[%s] Errore serializzazione JSON: %v", cfg.SensorID, err)
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = kafkaWriter.WriteMessages(ctx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytes})
			cancel()

			if err != nil {
				log.Printf("[%s] Errore invio Kafka: %v", cfg.SensorID, err)
			} else {
				fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
			}
		}
	}
}
