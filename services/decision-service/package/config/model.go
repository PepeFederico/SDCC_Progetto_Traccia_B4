package config

import (
	"encoding/json"
	"time"
)

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

type MessageStreamEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"` // Presente solo se Type == "DATA"
	Marker  *LatencyMarker  `json:"marker,omitempty"`  // Presente solo se Type == "LATENCY_MARKER"
}

type LatencyMarker struct {
	IngressTimestampNano int64 `json:"ingress_ts_nano"`
}

type SensorConfig struct {
	SensorID         string        `json:"sensorId"`
	Type             string        `json:"type"`
	MachineToControl string        `json:"machineToControl"`
	BaseMean         float64       `json:"baseMean"`
	Variance         float64       `json:"variance"`
	Interval         time.Duration `json:"interval"`

	SogliaMinima  float64 `json:"soglia_minima"`
	SogliaMassima float64 `json:"soglia_massima"`
	MaxStdDev     float64 `json:"max_stddev"`
	MaxDrift      float64 `json:"max_drift"`
}

type OperationalMode string

const (
	ModeStop OperationalMode = "ModeStop"
)

type AlarmMessage struct {
	SensorID string          `json:"sensorId"`
	Command  OperationalMode `json:"command"`
}

type RedisParameter struct {
	Address   string
	Password  string
	DefaultDB int
}
