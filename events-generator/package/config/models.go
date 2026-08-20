package config

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// OperationalMode : Definisce i possibili stati del sensore
type OperationalMode string

const (
	ModeNormal OperationalMode = "ModeNormal"
	ModeDrift  OperationalMode = "ModeDrift"
	ModeSpike  OperationalMode = "ModeSpike"
	ModeStop   OperationalMode = "ModeStop"
)

// SensorConfig : Definisce la configurazione iniziale di un sensore
//
//	MachineID	: Nome della macchina
//	BaseMean	: Valore medio
//	Variance	: Varianza
//	Interval	: Rate di campionamento
type SensorConfig struct {
	SensorID         string        `json:"sensorId"`
	Type             string        `json:"type"`
	MachineToControl string        `json:"machineToControl"`
	BaseMean         float64       `json:"baseMean"`
	Variance         float64       `json:"variance"`
	Interval         time.Duration `json:"interval"`
}

// StateCommand : Rappresenta un messaggio di controllo/cambio stato
type StateCommand struct {
	Mode           OperationalMode `json:"mode"`
	DriftRate      float64         `json:"driftRate"`
	SpikeMagnitude float64         `json:"spikeMagnitude"`
	SensorID       string          `json:"sensorId"`
}

// TemperatureReading : Rappresenta la lettura telemetrica inviata dal sensore
type TemperatureReading struct {
	MessageID   string  `json:"messageId"`
	SensorID    string  `json:"sensorId"`
	MachineID   string  `json:"machineId"`
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature_celsius"`
}

// PressureReading : Rappresenta la lettura telemetrica inviata dal sensore
type PressureReading struct {
	MessageID string  `json:"messageId"`
	SensorID  string  `json:"sensorId"`
	MachineID string  `json:"machineId"`
	Timestamp string  `json:"timestamp"`
	Pressure  float64 `json:"pressure_bar"`
}

type EmergencyCommand struct {
	MachineID string          `json:"machineId"`
	Command   OperationalMode `json:"command"`
}

type DashboardServer struct {
	TempWriter   *kafka.Writer
	PressWriter  *kafka.Writer
	SignalWriter *kafka.Writer
}

func NewDashboardServer(temp, press, signal *kafka.Writer) *DashboardServer {
	return &DashboardServer{
		TempWriter:   temp,
		PressWriter:  press,
		SignalWriter: signal,
	}
}
