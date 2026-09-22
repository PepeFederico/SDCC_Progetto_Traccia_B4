package kafka

import (
	"context"
	"decision-service/package/config"
	"decision-service/package/storage"
	"encoding/json"
	"fmt"
	"log"
	"math"
	metrics "progettoSDCC/prometheus"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func StartDecisionConsumer(ctx context.Context, broker, readerTopic, writerTopic, groupID string, conn *redis.Client, m *metrics.MetricsFactory) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    readerTopic,
		GroupID:  groupID,
		MinBytes: 10,
		MaxBytes: 10e6,
	})

	writer := newKafkaProducer(broker, writerTopic)

	go func() {
		defer func() {
			_ = reader.Close()
			_ = writer.Close()
		}()

		fmt.Printf("Consumer Attivo sul topic '%s'...\n", readerTopic)

		for {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				log.Printf("Errore lettura Kafka: %v", err)
				continue
			}

			var event config.MessageStreamEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Errore unmarshal MessageStreamEvent (messaggio scartato): %v", err)
				_ = reader.CommitMessages(ctx, msg)
				continue
			}

			// -----------------------------------------------------------------
			// CASO A: Gestione del LATENCY_MARKER
			// -----------------------------------------------------------------
			if event.Type == "LATENCY_MARKER" && event.Marker != nil {
				nowNano := time.Now().UnixNano()
				totalLatencyMs := float64(nowNano-event.Marker.IngressTimestampNano) / 1e6

				log.Printf("[LATENCY E2E] Latenza Totale Pipeline: %.2f ms", totalLatencyMs)

				// 1. Misura Latenza End-to-End su Prometheus (convertita in secondi)
				m.E2ELatencyHistogram.Observe(totalLatencyMs / 1000.0)

				// 2. Incrementa Throughput Marker
				m.ProcessedEventsTotal.WithLabelValues("LATENCY_MARKER", "success").Inc()

				if err := reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[DECISION ERROR] Errore commit offset marker: %v", err)
				}
				continue
			}

			// -----------------------------------------------------------------
			// CASO B: Gestione del DATO SENSORIALE (DATA)
			// -----------------------------------------------------------------
			if event.Type == "DATA" && len(event.Payload) > 0 {
				var message config.ProcessedData
				if err = json.Unmarshal(event.Payload, &message); err != nil {
					log.Printf("Errore unmarshal payload ProcessedData (messaggio scartato): %v", err)
					m.ProcessedEventsTotal.WithLabelValues("DATA", "unmarshal_payload_error").Inc()
					_ = reader.CommitMessages(ctx, msg)
					continue
				}

				// Fallback sulla Key di Kafka se SensorID è vuoto nel payload
				sensorID := message.SensorID
				if sensorID == "" {
					sensorID = string(msg.Key)
				}

				// 3. Misura il Processing Lag (Backpressure tra Ingress e Sink)
				if !msg.Time.IsZero() {
					lagSeconds := time.Since(msg.Time).Seconds()
					m.ProcessingLagGauge.WithLabelValues(sensorID).Set(lagSeconds)
				}

				sensorCfg, err := storage.LoadParameterSensor(ctx, sensorID, conn)
				if err != nil {
					log.Printf("[DECISION WARNING] Parametri non trovati su Redis per %s: %v. Scarto evento.", sensorID, err)
					m.ProcessedEventsTotal.WithLabelValues("DATA", "redis_config_missing").Inc()
					_ = reader.CommitMessages(ctx, msg)
					continue
				}

				// Valutazione delle Regole di Decisione
				shouldStop, reason := evaluateAnomalies(message, sensorCfg)

				if shouldStop {
					log.Printf("[DECISION ALARM] Anomalia rilevata sul sensore %s! Motivo: %s", sensorID, reason)

					cmd := config.AlarmMessage{
						SensorID: sensorID,
						Command:  config.ModeStop,
					}

					if err := writeToKafka(sensorID, cmd, writer); err != nil {
						log.Printf("[DECISION ALARM] Errore invio allarme Kafka: %v", err)
					}
				}

				// Commit finale dell'offset processato
				if err := reader.CommitMessages(ctx, msg); err != nil {
					log.Printf("[DECISION ERROR] Errore commit offset Kafka: %v", err)
				}

				// 4. Incrementa Throughput Dati
				m.ProcessedEventsTotal.WithLabelValues("DATA", "success").Inc()
				continue
			}

			// Commit per eventuali tipi di evento non gestiti
			_ = reader.CommitMessages(ctx, msg)
			m.ProcessedEventsTotal.WithLabelValues("UNHANDLED", "ignored").Inc()
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
