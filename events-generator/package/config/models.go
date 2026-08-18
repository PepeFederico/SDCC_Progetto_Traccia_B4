package config

import (
	"time"
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
	SensorID         string
	Type             string
	MachineToControl string
	BaseMean         float64
	Variance         float64
	Interval         time.Duration
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
