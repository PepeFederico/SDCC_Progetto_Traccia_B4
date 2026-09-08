package main

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// Import del pacchetto generato
	generatorpb "progettoSDCC/proto/event-generator"
)

type Gateway struct {
	genClient generatorpb.EventGeneratorClient
}

//go:embed web-page/*
var embeddedFiles embed.FS

func main() {
	r := gin.Default()

	// 1. Parsing dei file HTML incorporati
	templ, err := template.Must(template.New("").ParseFS(embeddedFiles, "web-page/*.html")), error(nil)
	if err != nil {
		log.Fatalf("Errore nel caricamento dei template embed: %v", err)
	}
	r.SetHTMLTemplate(templ)

	// 2. Creazione della sotto-FS per estrarre il contenuto di "web-page"
	subFS, err := fs.Sub(embeddedFiles, "web-page")
	if err != nil {
		log.Fatalf("Errore nella creazione della sub-FS: %v", err)
	}
	fileServer := http.FileServer(http.FS(subFS))

	// 4. Rotta principale per HTML
	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", gin.H{})
	})

	// Servizio per CSS, JS e risorse statiche direttamente dalla root ("/")
	r.Use(func(c *gin.Context) {
		// Se la richiesta è per la root o per un'API, lascia gestire alle rotte Gin
		if c.Request.URL.Path == "/" || len(c.Request.URL.Path) >= 4 && c.Request.URL.Path[:4] == "/api" {
			c.Next()
			return
		}

		// Se il file esiste nella cartella web-page, servilo direttamente
		filePath := c.Request.URL.Path[1:]
		if _, err := fs.Stat(subFS, filePath); err == nil {
			fileServer.ServeHTTP(c.Writer, c.Request)
			c.Abort()
			return
		}

		c.Next()
	})

	// 3. Risoluzione dinamica dello host gRPC per Docker
	grpcHost := os.Getenv("GENERATOR_GRPC_HOST")
	if grpcHost == "" {
		grpcHost = "localhost:50051" // Fallback per l'esecuzione locale senza Docker
	}

	log.Printf("[API-Gateway] Connessione al servizio gRPC su: %s", grpcHost)
	conn, err := grpc.Dial(grpcHost, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Impossibile connettersi al Generatore gRPC: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		if err := conn.Close(); err != nil {
			log.Printf("Errore durante la chiusura della connessione gRPC: %v", err)
		}
	}(conn)

	gateway := &Gateway{
		genClient: generatorpb.NewEventGeneratorClient(conn),
	}

	// 5. Endpoints API
	r.POST("/api/sensors", gateway.handleCreateSensor)
	r.GET("/api/sensors", gateway.handleRetrieveActiveSensor)

	r.GET("/api/sensor-types", gateway.handleRetrieveType)
	r.POST("/api/command", gateway.handleSendCommand)

	// 6. Avvio server HTTP su 0.0.0.0 per rendere visibile la porta fuori dal container
	fmt.Println("[API-Gateway] Server HTTP in ascolto sulla porta :8080...")
	if err := r.Run("0.0.0.0:8080"); err != nil {
		log.Fatalf("Errore arresto API Gateway: %v", err)
	}
}

func (gw *Gateway) handleCreateSensor(c *gin.Context) {
	var body struct {
		SensorID         string  `json:"sensorId"`
		Type             string  `json:"type"`
		MachineToControl string  `json:"machineToControl"`
		BaseMean         float64 `json:"baseMean"`
		Variance         float64 `json:"variance"`
		IntervalSec      int64   `json:"interval"` // Riceve i secondi dal form
		SogliaMinima     float64 `json:"soglia_minima"`
		SogliaMassima    float64 `json:"soglia_massima"`
		MaxStdDev        float64 `json:"max_std_dev"`
		MaxDrift         float64 `json:"max_drift"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Conversione Secondi -> Nanosecondi per gRPC / time.Duration
	intervalNano := (time.Duration(body.IntervalSec) * time.Second).Nanoseconds()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := gw.genClient.InsertNewSensor(ctx, &generatorpb.Sensor{
		SensorId:         body.SensorID,
		Type:             body.Type,
		MachineToControl: body.MachineToControl,
		BaseMean:         float32(body.BaseMean),
		Variance:         float32(body.Variance),
		IntervalNano:     intervalNano,
		SogliaMinima:     float32(body.SogliaMinima),
		SogliaMassima:    float32(body.SogliaMassima),
		MaxStdDev:        float32(body.MaxStdDev),
		MaxDrift:         float32(body.MaxDrift),
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Errore gRPC: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": resp.GetMessage()})
}

func (gw *Gateway) handleRetrieveType(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := gw.genClient.LoadSensorType(ctx, &generatorpb.LoadSensorTypeRequest{})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Errore gRPC: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"sensor_types": resp.GetType()})
}

func (gw *Gateway) handleRetrieveActiveSensor(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := gw.genClient.RetriveActiveSensor(ctx, &generatorpb.GetActiveSensorRequest{})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Errore gRPC: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"active_sensors": resp.GetSensor()})
}

func (gw *Gateway) handleSendCommand(c *gin.Context) {
	var body struct {
		Mode           string  `json:"mode"`
		DriftRate      float32 `json:"driftRate"`
		SpikeMagnitude float64 `json:"spikeMagnitude"`
		SensorID       string  `json:"sensorId"`
	}

	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := gw.genClient.ChangeMode(ctx, &generatorpb.Mode{
		SensorId: body.SensorID,
		Value:    body.DriftRate,
		Mode:     body.Mode,
	})

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Errore gRPC: " + err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": resp.GetMessage()})
}
