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
