package config

import (
	"time"
)

type GenericTelemetry struct {
	MessageID          string  `json:"messageId"`
	SensorID           string  `json:"sensorId"`
	MachineID          string  `json:"machineId"`
	Timestamp          string  `json:"timestamp"`
	TemperatureCelsius float64 `json:"temperature_celsius,omitempty"`
	PressureBar        float64 `json:"pressure_bar,omitempty"`
}

type RedisParameter struct {
	Address   string
	Password  string
	DefaultDB int
}

/*
SensorState : 	Struct necessaria per la gestione del checkpointing della finestra

	Non salviamo nello stato risultati parziali. Saranno ricalcolati nel momento dell'eventuale reload dello stato
*/
type SensorState struct {
	SensorID       string    `json:"sensor_id"`
	MachineID      string    `json:"machine_id"`
	MaxEventTime   time.Time `json:"max_event_time"`
	Watermark      time.Time `json:"watermark"`
	LastEvaluation time.Time `json:"last_evaluation"`
	Buffer         []Item    `json:"buffer"`
}

type Item struct {
	SensorID  string
	MachineID string
	Timestamp time.Time
	Value     float64
}

type SlidingWindow struct {
	Dimension     time.Duration
	SlideInterval time.Duration
	ListItems     []Item
	StartTime     time.Time
	EndTime       time.Time
}

type ProcessedData struct {
	MessageID    string  `json:"messageId"`
	SensorID     string  `json:"sensorId"`
	MachineID    string  `json:"machineId"`
	Minimo       float64 `json:"minimo"`
	Media        float64 `json:"media"`
	Massimo      float64 `json:"massimo"`
	StdDev       float64 `json:"std-dev"`
	RateOfChange float64 `json:"rate-of-change"`
	Timestamp    string  `json:"timestamp"`
	WindowStart  string  `json:"windowStart"`
	WindowEnd    string  `json:"windowEnd"`
}

type SensorChannels struct {
	InputChannel       chan Item
	InvalidDataChannel chan string
}
