package analyzer

import (
	model "analyzer-service/package/config"
	"log"
	"math"
	"sync"
	"time"
)

// WorkerInstance : Gestisce lo stato per ogni sensore correntemente attivo
type WorkerInstance struct {
	mu          sync.RWMutex
	inputStream <-chan model.Item
	state       model.SensorState
}

// Mappa thread-safe dei worker correntemente attivi
var activeWorkers sync.Map

func SensorWorker(sensorID string, inputStream <-chan model.Item, restoredState *model.SensorState) {
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

	var mean, m2, massimo, minimo float64

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

		n := float64(i + 1)
		delta := el.Value - mean
		mean += delta / n
		delta2 := el.Value - mean
		m2 += delta2 * delta
	}

	var variance, std float64
	variance = m2 / float64(len(w.ListItems))
	std = math.Sqrt(variance)

	/*
		TODO:
			1)	Calcolo approfondito delle metriche --> DONE!
			2)	Identificazione di eventuali trend
			3)	Scrittura sul DB Reader
			4)	Implementazione gRPC --> Decision Service e Sink Service (Scrittura sul DB Reader -- Parte Query del CQRS Pattern)
	*/

	log.Printf("[WINDOW EVAL] Sensore: %s | Macchina : %s | Items: %d | Minimo: %.2f | Media: %.2f | Massimo:  %.2f | std-dev: %.2f | Win: [%s - %s] | Watermark: %s",
		sensorID,
		machineID,
		len(w.ListItems),
		minimo,
		mean,
		massimo,
		std,
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
