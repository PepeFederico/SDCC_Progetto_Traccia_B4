package sensor

import (
	"log"
	"sync"

	"progettoSDCC/events-generator/package/config"

	"github.com/segmentio/kafka-go"
)

var machineRegistry sync.Map

func StartSensor(cfg config.SensorConfig, stopChan <-chan struct{}, kafkaWriter *kafka.Writer) chan config.StateCommand {
	modeChannel := make(chan config.StateCommand, 10)
	machineRegistry.Store(cfg.SensorID, modeChannel)

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
