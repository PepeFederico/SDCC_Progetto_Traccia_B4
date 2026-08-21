package config

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

// SensorData : Definito un'interfaccia per astrarre l'accesso al valore numerico
//
//	e la modifica del timestamp, senza dover conoscere il tipo concreto sottostante
type SensorData interface {
	GetMeasure() float64
	SetTimestamp(ts string)
}

func (t *TemperatureReading) GetMeasure() float64    { return t.Temperature }
func (t *TemperatureReading) SetTimestamp(ts string) { t.Timestamp = ts }

func (p *PressureReading) GetMeasure() float64    { return p.Pressure }
func (p *PressureReading) SetTimestamp(ts string) { p.Timestamp = ts }
