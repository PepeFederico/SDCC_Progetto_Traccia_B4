package kafka

import (
	"context"
	"decision-service/package/config"
	"decision-service/package/storage"
	"encoding/json"
	"fmt"
	"log"
	"math"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func StartDecisionConsumer(ctx context.Context, broker, readerTopic, writerTopic, groupID string, conn *redis.Client) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    readerTopic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6,
	})

	writer := newKafkaProducer(broker, writerTopic)

	//	Inizio Logica del Servizio
	go func() {
		defer func() {
			_ = reader.Close()
			_ = writer.Close()
		}()

		fmt.Printf("Consumer Attivo sul topic '%s...\n", readerTopic)

		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Printf("Errore lettura Kafka: %v", err)
				continue
			}

			var message config.ProcessedData
			if err = json.Unmarshal(msg.Value, &message); err != nil {
				log.Printf("Errore unmarshal (messaggio scartato): %v", err)
				_ = reader.CommitMessages(ctx, msg)
				continue
			}

			// 	Caricamento parametri dal DB Redis
			sensorCfg, err := storage.LoadParameterSensor(ctx, message.SensorID, conn)
			if err != nil {
				log.Printf("[DECISION WARNING] Parametri non trovati su Redis per %s: %v. Scarto evento.", message.SensorID, err)
				_ = reader.CommitMessages(ctx, msg)
				continue
			}

			// 		Valutazione delle Regole di Decisione
			shouldStop, reason := evaluateAnomalies(message, sensorCfg)

			// 		Invio eventuale Segnale di Emergenza
			if shouldStop {
				log.Printf("[DECISION ALARM] Anomalia rilevata sul sensore %s! Motivo: %s", message.SensorID, reason)

				cmd := config.AlarmMessage{
					SensorID: message.SensorID,
					Command:  config.ModeStop,
				}

				if err := writeToKafka(message.SensorID, cmd, writer); err != nil {
					log.Printf("[DECISION ALARM] Errore invio allarme Kafka: %v", err)
				}
			}

			// 		Commit finale dell'offset processato
			if err := reader.CommitMessages(ctx, msg); err != nil {
				log.Printf("[DECISION ERROR] Errore commit offset Kafka: %v", err)
			}
		}
	}()

}

// Funzione ausiliaria pura per valutare la presenza di anomalie
func evaluateAnomalies(data config.ProcessedData, cfg config.SensorConfig) (bool, string) {
	// 		Controllo 1: Soglia Fuori Scala (Superamento limiti operativi Min/Max)
	if data.Media < cfg.SogliaMinima || data.Media > cfg.SogliaMassima {
		return true, fmt.Sprintf("Valore medio fuori soglia (Valore: %.2f, Limiti: [%.2f, %.2f])", data.Media, cfg.SogliaMinima, cfg.SogliaMassima)
	}

	// 		Controllo 2: Instabilità (StdDev eccessiva)
	if cfg.MaxStdDev > 0 && data.StdDev > cfg.MaxStdDev {
		return true, fmt.Sprintf("Instabilità elevata (StdDev: %.2f, Max Consolidata: %.2f)", data.StdDev, cfg.MaxStdDev)
	}

	// 		Controllo 3: Deriva Anomala (Trend/Drift)
	if cfg.MaxDrift > 0 && math.Abs(data.RateOfChange) > cfg.MaxDrift {
		return true, fmt.Sprintf("Deriva termica/pressione eccessiva (Drift: %.4f, Max Consolidata: %.4f)", math.Abs(data.RateOfChange), cfg.MaxDrift)
	}

	return false, ""
}
