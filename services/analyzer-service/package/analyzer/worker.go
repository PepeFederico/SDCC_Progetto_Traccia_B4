package analyzer

import (
	model "analyzer-service/package/config"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/segmentio/kafka-go"
)

// WorkerInstance : Gestisce lo stato per ogni sensore correntemente attivo
type WorkerInstance struct {
	mu          sync.RWMutex
	inputStream <-chan model.Item
	state       model.SensorState
}

// Mappa thread-safe dei worker correntemente attivi
var activeWorkers sync.Map
var producer *kafka.Writer

func generateULID() string {
	return ulid.Make().String()
}

func SensorWorker(sensorID string, inputStream <-chan model.Item, restoredState *model.SensorState, writer *kafka.Writer) {
	w := &WorkerInstance{
		inputStream: inputStream,
		state: model.SensorState{
			SensorID: sensorID,
			Buffer:   make([]model.Item, 0),
		},
	}

	if restoredState != nil {
		w.state = *restoredState
		log.Printf("[RECOVERY OK] Worker %s ripristinato da Redis con %d elementi nel buffer",
			sensorID, len(w.state.Buffer))
	}

	activeWorkers.Store(sensorID, w)
	//		Settiamo il Producer per ogni goroutine
	producer = writer

	go w.run()
}

func (w *WorkerInstance) run() {

	windowDuration := 2 * time.Minute
	slideInterval := 1 * time.Minute  //+ 30*time.Second
	watermarkDelay := 1 * time.Minute // Tolleranza ritardi di 1 minuto

	for item := range w.inputStream {
		eventTime := item.Timestamp.UTC()

		//	-----	Starting Sezione Critica	-----
		w.mu.Lock()

		w.state.MachineID = item.MachineID

		//	Aggiornamento MaxEventTime e Watermark
		if eventTime.After(w.state.MaxEventTime) {
			w.state.MaxEventTime = eventTime
			w.state.Watermark = w.state.MaxEventTime.Add(-watermarkDelay)
		}

		//	Controllo elementi in ritardo rispetto alla finestra
		if !w.state.Watermark.IsZero() && eventTime.Before(w.state.Watermark) {
			log.Printf("[LATE DATA SCARTATO] Sensore %s: eventTime=%s < watermark=%s",
				item.SensorID, eventTime.Format(time.RFC3339), w.state.Watermark.Format(time.RFC3339))
			w.mu.Unlock()
			continue //	Nuova iterazione del ciclo for
		}

		w.state.Buffer = append(w.state.Buffer, item)
		//	Pulizia elementi fuori finestra
		windowEnd := w.state.MaxEventTime
		windowStart := windowEnd.Add(-windowDuration)

		indexValid := 0
		for i, el := range w.state.Buffer {
			if el.Timestamp.After(windowStart) || el.Timestamp.Equal(windowStart) {
				indexValid = i
				break
			}
		}

		w.state.Buffer = w.state.Buffer[indexValid:]

		//	Verifico la chiusura della finestra
		var windowToProcess *model.SlidingWindow
		var currentWatermark time.Time

		if w.state.LastEvaluation.IsZero() || w.state.MaxEventTime.Sub(w.state.LastEvaluation) >= slideInterval {
			itemToProcess := make([]model.Item, len(w.state.Buffer))
			copy(itemToProcess, w.state.Buffer)

			windowToProcess = &model.SlidingWindow{
				Dimension:     windowDuration,
				SlideInterval: slideInterval,
				ListItems:     itemToProcess,
				StartTime:     windowStart,
				EndTime:       windowEnd,
			}
			currentWatermark = w.state.Watermark
			w.state.LastEvaluation = w.state.MaxEventTime
		}

		w.mu.Unlock()

		// Processamento della finestra --> Calcolo delle metriche
		if windowToProcess != nil {
			processWindow(w.state.SensorID, w.state.MachineID, *windowToProcess, currentWatermark)
		}

	}

	activeWorkers.Delete(w.state.SensorID)
}

func processWindow(sensorID, machineID string, w model.SlidingWindow, currentWatermark time.Time) {
	if len(w.ListItems) == 0 {
		return
	}

	var mean, m2, massimo, minimo, variance, std float64
	var sumT, sumV, sumTV, sumT2 float64

	firstTime := w.ListItems[0].Timestamp
	massimo = math.Inf(-1) //	Inizializzo con - infinito
	minimo = math.Inf(1)   //	Inizializzo con + infinito

	for i, el := range w.ListItems {

		//	Calcolo del Massimo e Minimo Valore
		if el.Value > massimo {
			massimo = el.Value
		}
		if el.Value < minimo {
			minimo = el.Value
		}

		//	Accumulo termini della regressione lineare
		t := el.Timestamp.Sub(firstTime).Seconds()
		v := el.Value

		sumT += t
		sumV += v
		sumTV += t * v
		sumT2 += t * t

		//	Algoritmo di Welford per calcolo della media e dev-std
		n := float64(i + 1)
		delta := el.Value - mean
		mean += delta / n
		delta2 := el.Value - mean
		m2 += delta2 * delta
	}

	variance = m2 / float64(len(w.ListItems))
	std = math.Sqrt(variance)

	var rateOfChange float64
	if len(w.ListItems) >= 2 {
		n := float64(len(w.ListItems))
		denominatore := (n * sumT2) - (sumT * sumT)
		if denominatore != 0 {
			rateOfChange = ((n * sumTV) - (sumT * sumV)) / denominatore
		}
	}

	payload := model.ProcessedData{
		MessageID:    generateULID(),
		SensorID:     sensorID,
		MachineID:    machineID,
		Minimo:       minimo,
		Media:        mean,
		Massimo:      massimo,
		StdDev:       std,
		RateOfChange: rateOfChange,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		WindowStart:  w.StartTime.Format("15:04:05"),
		WindowEnd:    w.EndTime.Format("15:04:05")}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ERRORE] Error marshalling payload: %s", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = producer.WriteMessages(ctx, kafka.Message{
		Value: jsonBytes,
	})
	cancel()

	if err != nil {
		log.Printf("[%s] Errore invio Kafka: %v", sensorID, err)
	} else {
		fmt.Printf("Messaggio inviato: %s\n", string(jsonBytes))
	}

	log.Printf("[WINDOW EVAL] Sensore: %s | Macchina : %s | Items: %d | Minimo: %.2f | Media: %.2f | Massimo:  %.2f | std-dev: %.2f | Tasso di Variazione: %.4f | Win: [%s - %s] | Watermark: %s",
		sensorID,
		machineID,
		len(w.ListItems),
		minimo,
		mean,
		massimo,
		std,
		rateOfChange,
		w.StartTime.Format("15:04:05"),
		w.EndTime.Format("15:04:05"),
		currentWatermark.Format("15:04:05"),
	)
}

func CaptureWindowSnapshot() map[string]model.SensorState {
	snapshot := make(map[string]model.SensorState)

	activeWorkers.Range(func(key, value any) bool {
		sensorID := key.(string)
		worker := value.(*WorkerInstance)

		worker.mu.RLock()
		defer worker.mu.RUnlock()

		bufCopy := make([]model.Item, len(worker.state.Buffer))
		copy(bufCopy, worker.state.Buffer)
		snapshot[sensorID] = model.SensorState{
			SensorID:       sensorID,
			MachineID:      worker.state.MachineID,
			MaxEventTime:   worker.state.MaxEventTime,
			Watermark:      worker.state.Watermark,
			LastEvaluation: worker.state.LastEvaluation,
			Buffer:         bufCopy,
		}

		return true
	})

	return snapshot
}
