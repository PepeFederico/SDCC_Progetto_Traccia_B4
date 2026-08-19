package sensor

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"progettoSDCC/events-generator/package/config"

	"github.com/segmentio/kafka-go"
)

//go:embed web-page/*
var webAssets embed.FS

func StartDashboardServer(port string, stopChan <-chan struct{}, telemetryWriter *kafka.Writer) {

	// 1	Endpoint /api/sensors (GET & POST)
	http.HandleFunc("/api/sensors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(GetActiveSensors())

		case http.MethodPost:
			var req struct {
				SensorID         string  `json:"sensorId"`
				Type             string  `json:"type"`
				MachineToControl string  `json:"machineToControl"`
				BaseMean         float64 `json:"baseMean"`
				Variance         float64 `json:"variance"`
				IntervalNs       int64   `json:"interval"`
			}

			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "Errore parsing JSON: "+err.Error(), http.StatusBadRequest)
				return
			}

			cfg := config.SensorConfig{
				SensorID:         req.SensorID,
				Type:             req.Type,
				MachineToControl: req.MachineToControl,
				BaseMean:         req.BaseMean,
				Variance:         req.Variance,
				Interval:         time.Duration(req.IntervalNs),
			}

			StartSensor(cfg, stopChan, telemetryWriter)
			log.Printf("[DEBUG MANAGER] Starting nuovo Sensore! [Macchina: %s -- Sensore: %s]", req.MachineToControl, req.SensorID)

			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, "Sensore %s creato con successo!", cfg.SensorID)

		default:
			http.Error(w, "Metodo non consentito", http.StatusMethodNotAllowed)
		}
	})

	// 2	Endpoint /api/sensor-types
	http.HandleFunc("/api/sensor-types", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Metodo non consentito", http.StatusMethodNotAllowed)
			return
		}

		types := []string{"TemperatureSensor", "PressureSensor"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(types)
	})

	// 3	Endpoint /api/command
	http.HandleFunc("/api/command", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Metodo non consentito", http.StatusMethodNotAllowed)
			return
		}

		var cmd config.StateCommand
		if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if success := SendControlCommand(cmd.SensorID, cmd); !success {
			http.Error(w, fmt.Sprintf("Sensore %s non trovato", cmd.SensorID), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "Comando inviato con successo a %s", cmd.SensorID)
	})

	// 4	Servizio File Statici usando fs.Sub
	// Estraiamo la sotto cartella "web-page" per farla diventare la radice del FileServer
	subFS, err := fs.Sub(webAssets, "web-page")
	if err != nil {
		log.Fatalf("Errore nel caricamento dei file statici %v", err)
	}

	// Registriamo il gestore per tutti gli altri percorsi statici
	http.Handle("/", http.FileServer(http.FS(subFS)))

	// 5	Avvio del Server HTTP
	log.Printf("[Dashboard] Server in ascolto su http://127.0.0.1:%s\n", port)
	go func() {
		if err := http.ListenAndServe("127.0.0.1:"+port, nil); err != nil {
			log.Fatalf("Errore avvio server dashboard: %v", err)
		}
	}()
}
