package storage

import "time"

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
