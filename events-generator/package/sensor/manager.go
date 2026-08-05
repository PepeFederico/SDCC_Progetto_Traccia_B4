package sensor

import (
	"fmt"
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
		ch.(chan config.StateCommand) <- cmd
		return true
	}
	fmt.Printf("Macchina %s non trovata nel registro\n", machineID)
	return false
}
