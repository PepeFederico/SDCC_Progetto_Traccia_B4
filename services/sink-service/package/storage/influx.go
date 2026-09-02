package storage

import (
	"log"

	influxdb "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type InfluxWriter struct {
	client   influxdb.Client
	writeAPI api.WriteAPI
}

func NewInfluxWriter(payload InfluxParameter) *InfluxWriter {
	opts := influxdb.DefaultOptions().
		SetBatchSize(10).           // Dimensione finestra del batch
		SetFlushInterval(5000).     // Flushing ogni 5 secondi (5000 ms)
		SetMaxRetries(5).           // Numero massimo di tentativi di retry prima di scartare il batch
		SetRetryInterval(1000).     // Intervallo iniziale di attesa (1 sec) prima del primo retry
		SetMaxRetryInterval(15000). // Intervallo massimo tra un retry e l'altro (15 sec)
		SetMaxRetryTime(60000)      // Tempo totale massimo concesso ai retry per un batch (60 sec)

	client := influxdb.NewClientWithOptions(payload.URl, payload.Token, opts)
	writeAPI := client.WriteAPI(payload.Org, payload.Bucket)

	go func() {
		for err := range writeAPI.Errors() {
			log.Printf("[INFLUXDB ERROR]: scrittura fallita definitivamente dopo i retry: %v", err)
		}
	}()

	return &InfluxWriter{
		client:   client,
		writeAPI: writeAPI,
	}
}

func (writer *InfluxWriter) WritePoints(payload *DataPoint) {
	p := influxdb.NewPoint(
		"sensor_metrics",
		map[string]string{
			"sensor_id":  payload.SensorID,
			"machine_id": payload.MachineID,
		},
		map[string]interface{}{
			"minimo":         payload.Minimo,
			"massimo":        payload.Massimo,
			"media":          payload.Media,
			"std-dev":        payload.StdDev,
			"rate-of-change": payload.RateOfChange,
		},
		payload.Timestamp,
	)

	writer.writeAPI.WritePoint(p)
}

func (writer *InfluxWriter) Close() {
	writer.writeAPI.Flush()
	writer.client.Close()
}
