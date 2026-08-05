package config

import (
	"time"
)

// OperationalMode : Definisce i possibili stati del sensore
type OperationalMode int

const (
	ModeNormal OperationalMode = iota //	Funzionamento operativo standard
	ModeSpike                         //	Genera un singolo picco anomalo e torna Normal
	ModeDrift                         //	Degrado progressivo del trend --> Simulazione anomalia
	ModeStop                          //	Interruzione Forzata Macchinario
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
	Mode           OperationalMode
	DriftRate      float64
	SpikeMagnitude float64
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
