package config

import "encoding/json"

// TemperatureReading rappresenta la lettura della temperatura
type TemperatureReading struct {
	MessageID   string  `json:"messageId"`
	SensorID    string  `json:"sensorId"`
	MachineID   string  `json:"machineId"`
	Timestamp   string  `json:"timestamp"`
	Temperature float64 `json:"temperature_celsius"`
}

// PressureReading rappresenta la lettura della pressione
type PressureReading struct {
	MessageID string  `json:"messageId"`
	SensorID  string  `json:"sensorId"`
	MachineID string  `json:"machineId"`
	Timestamp string  `json:"timestamp"`
	Pressure  float64 `json:"pressure_bar"`
}

type MessageStreamEvent struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"` // Presente solo se Type == "DATA"
	Marker  *LatencyMarker  `json:"marker,omitempty"`  // Presente solo se Type == "LATENCY_MARKER"
}

type LatencyMarker struct {
	IngressTimestampNano int64 `json:"ingress_ts_nano"`
}

// SensorData : Definito un'interfaccia per astrarre l'accesso al valore numerico
//
//	e la modifica del timestamp, senza dover conoscere il tipo concreto sottostante
type SensorData interface {
	GetMeasure() float64
	SetTimestamp(ts string)
	GetSensorID() string
}

func (t *TemperatureReading) GetMeasure() float64    { return t.Temperature }
func (t *TemperatureReading) SetTimestamp(ts string) { t.Timestamp = ts }
func (t *TemperatureReading) GetSensorID() string    { return t.SensorID }

func (p *PressureReading) GetMeasure() float64    { return p.Pressure }
func (p *PressureReading) SetTimestamp(ts string) { p.Timestamp = ts }
func (p *PressureReading) GetSensorID() string    { return p.SensorID }
