package sensor

import (
	"context"
	"encoding/json"
	"fmt"
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

	fmt.Printf("[Macchina: %s -- Sensore: %s] Avvio monitoraggio (Media: %.2f°C)\n", cfg.MachineToControl, cfg.SensorID, cfg.BaseMean)

	for {
		select {
		case <-stopChan:
			fmt.Printf("[%s] Arresto sensore.\n", cfg.SensorID)
			return

		case cmd := <-modeChan:
			currentMode = cmd.Mode
			switch cmd.Mode {
			case config.ModeStop:
				fmt.Printf("[Macchina: %s -- Sensore: %s] Emergenza! Arresto preventivo.\n", cfg.MachineToControl, cfg.SensorID)

				//	Simulazione riparazione guasto
				var timeout = randomRange(30, 120)
				time.Sleep(time.Duration(timeout) * time.Second)

				//	Ripristino corretto funzionamento
				currentMode = config.ModeNormal
			case config.ModeDrift:
				driftRate = cmd.DriftRate
			case config.ModeSpike:
				spikeValue = cmd.SpikeMagnitude
			default:
				currentMean = cfg.BaseMean
				driftRate = 0.0
			}

		case <-ticker.C:
			noise := stdDev * rand.NormFloat64()
			switch currentMode {
			case config.ModeDrift:
				currentMean += driftRate
			case config.ModeSpike:
				noise += spikeValue
				currentMode = config.ModeNormal
			default:

			}

			temp := currentMean + noise
			payload := config.TemperatureReading{
				MessageID:   generateULID(),
				SensorID:    cfg.SensorID,
				MachineID:   cfg.MachineToControl,
				Timestamp:   time.Now().UTC().Format(time.RFC3339),
				Temperature: math.Round(temp*100) / 100,
			}

			jsonBytes, _ := json.Marshal(payload)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = kafkaWriter.WriteMessages(ctx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytes})
			cancel()

			fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
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

	fmt.Printf("[Macchina: %s -- Sensore: %s] Avvio monitoraggio (Media: %.2fBar)\n", cfg.MachineToControl, cfg.SensorID, cfg.BaseMean)

	for {
		select {
		case <-stopChan:
			fmt.Printf("[%s] Arresto sensore.\n", cfg.SensorID)
			return

		case cmd := <-modeChan:
			currentMode = cmd.Mode
			switch cmd.Mode {
			case config.ModeStop:
				fmt.Printf("[Macchina: %s -- Sensore: %s] Emergenza! Arresto preventivo.\n", cfg.MachineToControl, cfg.SensorID)

				//	Simulazione riparazione guasto
				var timeout = randomRange(30, 120)
				time.Sleep(time.Duration(timeout) * time.Second)

				//	Ripristino corretto funzionamento
				currentMode = config.ModeNormal
			case config.ModeDrift:
				driftRate = cmd.DriftRate
			case config.ModeSpike:
				spikeValue = cmd.SpikeMagnitude
			default:
				currentMean = cfg.BaseMean
				driftRate = 0.0
			}

		case <-ticker.C:
			noise := stdDev * rand.NormFloat64()
			switch currentMode {
			case config.ModeDrift:
				currentMean += driftRate
			case config.ModeSpike:
				noise += spikeValue
				currentMode = config.ModeNormal
			default:

			}

			temp := currentMean + noise
			payload := config.PressureReading{
				MessageID: generateULID(),
				SensorID:  cfg.SensorID,
				MachineID: cfg.MachineToControl,
				Timestamp: time.Now().UTC().Format(time.RFC3339),
				Pressure:  math.Round(temp*100) / 100,
			}

			jsonBytes, _ := json.Marshal(payload)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = kafkaWriter.WriteMessages(ctx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytes})
			cancel()

			fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
		}
	}
}
