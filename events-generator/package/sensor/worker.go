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
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func generateULID() string {
	return ulid.Make().String()
}
func randomRange(min, max int) int {
	return min + rand.IntN(max-min+1)
}

func temperatureSensorWorker(ctx context.Context, cfg config.SensorConfig, stopChan <-chan struct{}, modeChan <-chan config.StateCommand, kafkaWriter *kafka.Writer, conn *redis.Client) {
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

	updateSensorState := updateState(ctx, cfg, conn)

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
				var done bool
				currentMode, currentMean, driftRate, done = processModeStopState(ctx, cfg, stopChan, updateSensorState, conn)
				if done {
					return
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

			if whatWeDo < 0.01 {
				// CASO 1: Dato Corrotto
				payload = config.TemperatureReading{
					MessageID:   generateULID(),
					SensorID:    cfg.SensorID,
					MachineID:   cfg.MachineToControl,
					Timestamp:   time.Now().UTC().Format(time.RFC3339),
					Temperature: -999.99,
				}

			} else if whatWeDo < 0.03 {
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

			msg := config.MessageStreamEvent{
				Type:    "DATA",
				Payload: jsonBytes,
			}

			jsonBytesMsg, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[%s] Errore serializzazione JSON: %v", cfg.SensorID, err)
				continue
			}

			kCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = kafkaWriter.WriteMessages(kCtx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytesMsg})
			cancel()

			if err != nil {
				log.Printf("[%s] Errore invio Kafka: %v", cfg.SensorID, err)
			} else {
				fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
			}
		}
	}
}

func pressureSensorWorker(ctx context.Context, cfg config.SensorConfig, stopChan <-chan struct{}, modeChan <-chan config.StateCommand, kafkaWriter *kafka.Writer, conn *redis.Client) {
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

	updateSensorState := updateState(ctx, cfg, conn)

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
				var done bool
				currentMode, currentMean, driftRate, done = processModeStopState(ctx, cfg, stopChan, updateSensorState, conn)
				if done {
					return
				}
			case config.ModeDrift:
				driftRate = cmd.DriftRate
			case config.ModeSpike:
				spikeValue = cmd.SpikeMagnitude
			case config.ModeNormal:
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

			if whatWeDo < 0.01 {
				// CASO 1 (1% delle volte): Dato Corrotto (es. NaN)
				payload = config.PressureReading{
					MessageID: generateULID(),
					SensorID:  cfg.SensorID,
					MachineID: cfg.MachineToControl,
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Pressure:  -999.99,
				}

			} else if whatWeDo < 0.03 {
				// CASO 2 (2% delle volte): Picco Fuori Scala (Outlier)
				outlierPressure := currentMean + (noise * 5) // Moltiplichiamo il rumore per generare un picco

				payload = config.PressureReading{
					MessageID: generateULID(),
					SensorID:  cfg.SensorID,
					MachineID: cfg.MachineToControl,
					Timestamp: time.Now().UTC().Format(time.RFC3339),
					Pressure:  math.Round(outlierPressure*100) / 100,
				}

			} else {
				// CASO 3 (97% delle volte): Flusso Normale
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

			msg := config.MessageStreamEvent{
				Type:    "DATA",
				Payload: jsonBytes,
			}

			jsonBytesMsg, err := json.Marshal(msg)
			if err != nil {
				log.Printf("[%s] Errore serializzazione JSON: %v", cfg.SensorID, err)
				continue
			}

			kCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = kafkaWriter.WriteMessages(kCtx, kafka.Message{Key: []byte(cfg.SensorID), Value: jsonBytesMsg})
			cancel()

			if err != nil {
				log.Printf("[%s] Errore invio Kafka: %v", cfg.SensorID, err)
			} else {
				fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
			}
		}
	}
}

func processModeStopState(ctx context.Context, cfg config.SensorConfig, stopChan <-chan struct{}, updateSensorState func(state string), conn *redis.Client) (config.OperationalMode, float64, float64, bool) {
	//	Ricevuto comando di STOP sul sensore SensorID
	log.Printf("[Macchina: %s -- Sensore: %s] Emergenza! Arresto preventivo.\n", cfg.MachineToControl, cfg.SensorID)

	//	Eseguo il cambio di stato del Sensore: Ready --> Stopped
	updateSensorState("STOPPED")
	//	Registro il momento in cui è stato Stoppato il sensore
	_ = conn.Set(ctx, fmt.Sprintf("sensor:stopped_at:%s", cfg.SensorID), time.Now().UTC().Format(time.RFC3339), 0).Err()

	// Simulazione riparazione senza bloccare lo stopChan
	timeout := time.Duration(randomRange(30, 120)) * time.Second
	select {
	case <-stopChan:
		log.Printf("[%s] Arresto sensore durante la riparazione.\n", cfg.SensorID)
		return "", 0, 0, true
	case <-time.After(timeout):
		// Ripristino corretto funzionamento al termine della riparazione
		log.Printf("[%s] Ripristino corretto funzionamento!. Avvio fase di WARM-UP. \n", cfg.SensorID)
	}

	//	Aggiornamento Stato
	updateSensorState("WARM-UP")
	warmupTimeout := 10 * time.Second

	select {
	case <-stopChan:
		log.Printf("[%s] Arresto sensore durante la fase di WARM-UP.\n", cfg.SensorID)
		return "", 0, 0, true
	case <-time.After(warmupTimeout):
		//	Aggiornamento Stato
		updateSensorState("READY")
		log.Printf("[%s] Ripristino corretto funzionamento!.\n", cfg.SensorID)
	}
	return config.ModeNormal, cfg.BaseMean, 0.0, false
}

func updateState(ctx context.Context, cfg config.SensorConfig, conn *redis.Client) func(state string) {
	updateSensorState := func(state string) {
		err := conn.Set(ctx, fmt.Sprintf("sensor:state:%s", cfg.SensorID), state, 0).Err()
		if err != nil {
			log.Printf("[%s] Errore salvataggio stato %s su Redis: %v", cfg.SensorID, state, err)
		}

		eventPayload := fmt.Sprintf("%s:%s", cfg.SensorID, state)
		conn.Publish(ctx, "sensor:state-events", eventPayload)
		log.Printf("[%s] Stato aggiornato: %s", cfg.SensorID, state)

		publishToDashboard(cfg, state, conn, ctx)
	}
	return updateSensorState
}

func publishToDashboard(cfg config.SensorConfig, state string, conn *redis.Client, ctx context.Context) {
	//	Pubblicazione per aggiornare la dashboard, sfruttando Redis Pub/Sub
	dashboardPayload := config.DashboardInfo{
		SensorID:  cfg.SensorID,
		MachineID: cfg.MachineToControl,
		Status:    state,
	}

	jsonBytes, err := json.Marshal(dashboardPayload)
	if err != nil {
		log.Printf("Error marshalling dashboardInfo : %v", err)
	}

	//	Salva lo stato nell'Hash Set globale che lo SSE legge al boot
	err = conn.HSet(ctx, "sensors:current_status", cfg.SensorID, string(jsonBytes)).Err()
	if err != nil {
		log.Printf("[%s] Errore aggiornamento HSet sensors:current_status: %v", cfg.SensorID, err)
	}

	conn.Publish(ctx, "sensor:status", string(jsonBytes))
}
