package config

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type OperationalMode string

const (
	ModeNormal OperationalMode = "ModeNormal"
	ModeDrift  OperationalMode = "ModeDrift"
	ModeSpike  OperationalMode = "ModeSpike"
	ModeStop   OperationalMode = "ModeStop"
)

type SensorConfig struct {
	SensorID         string        `json:"sensorId"`
	Type             string        `json:"type"`
	MachineToControl string        `json:"machineToControl"`
	BaseMean         float64       `json:"baseMean"`
	Variance         float64       `json:"variance"`
	Interval         time.Duration `json:"interval"`

	SogliaMinima  float64 `json:"soglia_minima"`
	SogliaMassima float64 `json:"soglia_massima"`
	MaxStdDev     float64 `json:"max_std_dev"`
	MaxDrift      float64 `json:"max_drift"`
}

type StateCommand struct {
	Mode           OperationalMode `json:"mode"`
	DriftRate      float64         `json:"driftRate"`
	SpikeMagnitude float64         `json:"spikeMagnitude"`
	SensorID       string          `json:"sensorId"`
}

type TemperatureReading struct {
	MessageID   string  `json:"messageId"`
	SensorID    string  `json:"sensorId"`
	MachineID   string  `json:"machineId"`
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature_celsius"`
}

type PressureReading struct {
	MessageID string  `json:"messageId"`
	SensorID  string  `json:"sensorId"`
	MachineID string  `json:"machineId"`
	Timestamp string  `json:"timestamp"`
	Pressure  float64 `json:"pressure_bar"`
}

type EmergencyCommand struct {
	SensorID string          `json:"sensorId"`
	Command  OperationalMode `json:"command"`
}

type DashboardServer struct {
	TempWriter   *kafka.Writer
	PressWriter  *kafka.Writer
	SignalWriter *kafka.Writer
	ConnRedis    *redis.Client
}

type RedisParameter struct {
	Address   string
	Password  string
	DefaultDB int
}

type DashboardInfo struct {
	SensorID  string `json:"sensorId"`
	MachineID string `json:"machineId"`
	Status    string `json:"status"`
}
