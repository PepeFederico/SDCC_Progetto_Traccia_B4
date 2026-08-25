package analyzer

import (
	model "analyzer-service/package/config"
	"log"
	"time"
)

func SensorWorker(inputStream <-chan model.Item) {
	windowDuration := 2 * time.Minute
	slideInterval := 1 * time.Minute  //+ 30*time.Second
	watermarkDelay := 1 * time.Minute // Tolleranza ritardi di 1 minuto

	var maxEventTime time.Time
	var watermark time.Time
	var lastEvaluation time.Time

	buffer := make([]model.Item, 0)

	for item := range inputStream {
		eventTime := item.Timestamp.UTC()

		//	Aggiorna il tempo massimo visto finora e ricalcola il Watermark
		if eventTime.After(maxEventTime) {
			maxEventTime = eventTime
			watermark = maxEventTime.Add(-watermarkDelay)
		}

		//	CONTROLLO WATERMARK: Se l'elemento è più vecchio del Watermark va scartato
		if !watermark.IsZero() && eventTime.Before(watermark) {
			log.Printf("[LATE DATA SCARTATO] Sensore %s: eventTime=%s < watermark=%s",
				item.SensorID, eventTime.Format(time.RFC3339), watermark.Format(time.RFC3339))
			continue
		}

		//	Se il dato rientra nella soglia, viene aggiunto al buffer temporale
		buffer = append(buffer, item)

		//	Calcola i limiti della finestra corrente in base al maxEventTime
		windowEnd := maxEventTime
		windowStart := windowEnd.Add(-windowDuration)

		//	Rimuovi dal buffer locale gli elementi usciti completamente dalla finestra
		firstValidIdx := 0
		for i, el := range buffer {
			if el.Timestamp.After(windowStart) || el.Timestamp.Equal(windowStart) {
				firstValidIdx = i
				break
			}
		}
		buffer = buffer[firstValidIdx:]

		//	Esegui la valutazione allo scatto del passo
		if lastEvaluation.IsZero() || maxEventTime.Sub(lastEvaluation) >= slideInterval {
			w := model.SlidingWindow{
				Dimension:     windowDuration,
				SlideInterval: slideInterval,
				ListItems:     make([]model.Item, len(buffer)),
				StartTime:     windowStart,
				EndTime:       windowEnd,
			}
			copy(w.ListItems, buffer)

			processWindow(item.SensorID, item.MachineID, w, watermark)
			lastEvaluation = maxEventTime
		}
	}
}

func processWindow(sensorID, machineID string, w model.SlidingWindow, currentWatermark time.Time) {
	if len(w.ListItems) == 0 {
		return
	}

	var sum float64
	for _, item := range w.ListItems {
		sum += item.Value
	}
	avg := sum / float64(len(w.ListItems))

	/*
		TODO:
			1)	Calcolo approfondito delle metriche
			2)	Identificazione di eventuali trend
			3)	Scrittura sul DB Reader
			4)	Implementazione gRPC --> Decision Service
	*/

	log.Printf("[WINDOW EVAL] Sensore: %s | Macchina : %s | Items: %d | Avg: %.2f | Win: [%s - %s] | Watermark: %s",
		sensorID,
		machineID,
		len(w.ListItems),
		avg,
		w.StartTime.Format("15:04:05"),
		w.EndTime.Format("15:04:05"),
		currentWatermark.Format("15:04:05"),
	)
}
