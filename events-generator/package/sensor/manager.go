package sensor

import (
	"context"
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

	switch cfg.Type {
	case "TemperatureSensor":
		go temperatureSensorWorker(cfg, stopChan, modeChannel, kafkaWriter)
	case "PressureSensor":
		go pressureSensorWorker(cfg, stopChan, modeChannel, kafkaWriter)
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
