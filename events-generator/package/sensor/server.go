package sensor

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"time"

	"progettoSDCC/events-generator/package/config"
)

//go:embed web-page/*
var webAssets embed.FS

func StartDashboardServer(port string, stopChan <-chan struct{}, topics *config.DashboardServer) {

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

			switch cfg.Type {
			case "TemperatureSensor":
				StartSensor(cfg, stopChan, topics.TempWriter)
			case "PressureSensor":
				StartSensor(cfg, stopChan, topics.PressWriter)
			default:
				http.Error(w, "Tipo sensore non supportato", http.StatusBadRequest)
				return
			}

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

	// 5. Avvio del Server HTTP con Graceful Shutdown
	server := &http.Server{Addr: "127.0.0.1:" + port}

	go func() {
		log.Printf("[Dashboard] Server in ascolto su http://127.0.0.1:%s\n", port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Errore avvio server dashboard: %v", err)
		}
	}()

	// Inizializza l'arresto pulito quando lo stopChan viene chiuso dal main
	go func() {
		<-stopChan
		log.Println("[Dashboard] Arresto server HTTP in corso...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			log.Printf("[Dashboard] Errore durante lo shutdown HTTP: %v", err)
		}
	}()
}
