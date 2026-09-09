package storage

import (
	"context"
	"fmt"
	"log"
	sinkpb "progettoSDCC/proto/sink-service"

	influxdb "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

type InfluxClient struct {
	client   influxdb.Client
	writeAPI api.WriteAPI
	readAPI  api.QueryAPI
	bucket   string
	org      string
}

func NewInfluxClient(payload InfluxParameter) *InfluxClient {
	opts := influxdb.DefaultOptions().
		SetBatchSize(10).           // Dimensione finestra del batch
		SetFlushInterval(5000).     // Flushing ogni 5 secondi (5000 ms)
		SetMaxRetries(5).           // Numero massimo di tentativi di retry prima di scartare il batch
		SetRetryInterval(1000).     // Intervallo iniziale di attesa (1 sec) prima del primo retry
		SetMaxRetryInterval(15000). // Intervallo massimo tra un retry e l'altro (15 sec)
		SetMaxRetryTime(60000)      // Tempo totale massimo concesso ai retry per un batch (60 sec)

	client := influxdb.NewClientWithOptions(payload.URl, payload.Token, opts)
	writeAPI := client.WriteAPI(payload.Org, payload.Bucket)
	readAPI := client.QueryAPI(payload.Org)

	go func() {
		for err := range writeAPI.Errors() {
			log.Printf("[INFLUXDB ERROR]: scrittura fallita definitivamente dopo i retry: %v", err)
		}
	}()

	return &InfluxClient{
		client:   client,
		writeAPI: writeAPI,
		readAPI:  readAPI,
		bucket:   payload.Bucket,
		org:      payload.Org,
	}
}

func (writer *InfluxClient) WritePoints(payload *DataPoint) {
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

// GetMetricsSnapshot Metodo di lettura che esegue la query Flux e mappa i dati nel protobuf gRPC
func (writer *InfluxClient) GetMetricsSnapshot(ctx context.Context) ([]*sinkpb.SensorMetric, error) {
	query := fmt.Sprintf(`
		import "math"

		from(bucket: "%s")
		  |> range(start: -3m)
		  |> filter(fn: (r) => r["_measurement"] == "sensor_metrics")
		  |> filter(fn: (r) => 
		      r["_field"] == "media" or 
		      r["_field"] == "minimo" or 
		      r["_field"] == "massimo" or 
		      r["_field"] == "std-dev" or 
		      r["_field"] == "rate-of-change"
		  )
		  |> group(columns: ["machine_id", "sensor_id", "_field"])
		  |> aggregateWindow(every: 2m, fn: mean, createEmpty: false)
		  |> last()
		  |> map(fn: (r) => ({ r with _value: math.round(x: r._value * 100000.0) / 100000.0 }))
		  |> pivot(rowKey:["machine_id", "sensor_id"], columnKey: ["_field"], valueColumn: "_value")
		  |> yield(name: "complete_metrics")
	`, writer.bucket)

	result, err := writer.readAPI.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("errore query InfluxDB: %w", err)
	}

	var metrics []*sinkpb.SensorMetric

	for result.Next() {
		record := result.Record()

		getFloat := func(key string) float64 {
			if val, ok := record.ValueByKey(key).(float64); ok {
				return val
			}
			return 0.0
		}

		machineID, _ := record.ValueByKey("machine_id").(string)
		sensorID, _ := record.ValueByKey("sensor_id").(string)

		metrics = append(metrics, &sinkpb.SensorMetric{
			MachineId:    machineID,
			SensorId:     sensorID,
			Media:        getFloat("media"),
			Minimo:       getFloat("minimo"),
			Massimo:      getFloat("massimo"),
			StdDev:       getFloat("std-dev"),
			RateOfChange: getFloat("rate-of-change"),
		})
	}

	if result.Err() != nil {
		return nil, result.Err()
	}

	return metrics, nil
}

func (writer *InfluxClient) Close() {
	writer.writeAPI.Flush()
	writer.client.Close()
}
