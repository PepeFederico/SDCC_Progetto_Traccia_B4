package storage

import (
	"encoding/json"
	"time"
)

type InfluxParameter struct {
	URl    string
	Token  string
	Org    string
	Bucket string
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

type MessageStreamEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"` // Presente solo se Type == "DATA"
	Marker  *LatencyMarker  `json:"marker,omitempty"`  // Presente solo se Type == "LATENCY_MARKER"
}

type LatencyMarker struct {
	IngressTimestampNano int64 `json:"ingress_ts_nano"`
}

type DataPoint struct {
	SensorID     string    `json:"sensorId"`
	MachineID    string    `json:"machineId"`
	Minimo       float64   `json:"minimo"`
	Media        float64   `json:"media"`
	Massimo      float64   `json:"massimo"`
	StdDev       float64   `json:"std-dev"`
	RateOfChange float64   `json:"rate-of-change"`
	Timestamp    time.Time `json:"timestamp"`
}
