package sensor

import (
	_ "embed" // Import necessario per la direttiva //go:embed
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"progettoSDCC/events-generator/package/config"
)

// Incorpora il file index.html
//
//go:embed index.html
var indexHTML []byte

func StartDashboardServer(port string) {
	// 1	Restituisce la lista dei SensorID attivi
	http.HandleFunc("/api/sensors", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(GetActiveSensors())
	})

	// 2	Riceve il comando dalla pagina web e invoca SendControlCommand
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

		// Utilizza la funzione SendControlCommand
		success := SendControlCommand(cmd.SensorID, cmd)
		if !success {
			http.Error(w, fmt.Sprintf("Sensore %s non trovato", cmd.SensorID), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "Comando inviato con successo a %s", cmd.SensorID)
	})

	// 3	Serve la pagina HTML direttamente dalla memoria
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})

	log.Printf("[Dashboard] Server in ascolto su http://localhost:%s\n", port)
	go func() {
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatalf("Errore avvio server dashboard: %v", err)
		}
	}()
}
