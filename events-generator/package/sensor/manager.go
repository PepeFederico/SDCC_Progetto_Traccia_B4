package sensor

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"progettoSDCC/events-generator/package/config"
	"progettoSDCC/events-generator/package/storage"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var machineRegistry sync.Map

func StartSensor(ctx context.Context, cfg config.SensorConfig, stopChan <-chan struct{}, kafkaWriter *kafka.Writer, conn *redis.Client) chan config.StateCommand {
	modeChannel := make(chan config.StateCommand, 10)
	machineRegistry.Store(cfg.SensorID, modeChannel)

	//	Salvataggio limiti operativi sul DB Redis, tentativo di Invio con Retry
	maxRetries := 5

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if storage.SaveParameter(ctx, cfg, conn) == nil {
			break
		}

		log.Printf("[WARNING]: Tentativo %d/%d fallito per sensore %s. Retry in corso", attempt, maxRetries, cfg.SensorID)
		time.Sleep(time.Duration(attempt*100) * time.Millisecond)
	}

	//	Gestione dello stato dei Sensori (All'avvio Stato : Ready)
	err := conn.Set(ctx, fmt.Sprintf("sensor:state:%s", cfg.SensorID), "READY", 0).Err()
	if err != nil {
		log.Printf("[%s] Errore salvataggio dello stato su Redis : %v", cfg.SensorID, err)
	}

	//	Pubblicazione per aggiornare la cache locale degli altri servizi sfruttando Pub/Sub Redis
	eventPayload := fmt.Sprintf("%s:%s", cfg.SensorID, "READY")
	conn.Publish(ctx, "sensor:state-events", eventPayload)

	log.Printf("[%s] Sensore registrato con stato: %s", cfg.SensorID, eventPayload)

	switch cfg.Type {
	case "TemperatureSensor":
		go temperatureSensorWorker(ctx, cfg, stopChan, modeChannel, kafkaWriter, conn)
	case "PressureSensor":
		go pressureSensorWorker(ctx, cfg, stopChan, modeChannel, kafkaWriter, conn)
	}

	return modeChannel
}

func SendControlCommand(machineID string, cmd config.StateCommand) bool {
	if ch, ok := machineRegistry.Load(machineID); ok {
		log.Printf("[DEBUG MANAGER] Inoltro comando %+v al sensore %s", cmd, machineID)
		ch.(chan config.StateCommand) <- cmd
		return true
	}
	log.Printf("[DEBUG MANAGER] Sensore %s NON trovato nel registro", machineID)
	return false
}

func GetActiveSensors() []string {
	var sensors []string
	machineRegistry.Range(func(key, value any) bool {
		if sensorID, ok := key.(string); ok {
			sensors = append(sensors, sensorID)
		}
		return true
	})
	return sensors
}
