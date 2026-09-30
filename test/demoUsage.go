// Demo: rampa controllata fino a ~500 sensori.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	generatorpb "progettoSDCC/proto/event-generator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type stageDemo struct {
	name        string
	sensorCount int           // Nuovi sensori da aggiungere in questa fase
	interval    time.Duration // Frequenza di invio dati per ciascun sensore
	holdTime    time.Duration // Quanto mantenere il carico prima della fase successiva
	concurrency int           // Goroutine parallele per le chiamate gRPC
}

func main() {
	baseAddr := flag.String("addr", "localhost:50051", "Indirizzo gRPC del generatore (es. IP:Porta o ClusterIP:Porta)")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("\n[STOP] Demo interrotta manualmente.")
		cancel()
	}()

	log.Printf("[CONNECT] Tentativo di connessione gRPC a: %s...", *baseAddr)

	// Utilizziamo WithBlock/dialContext implicito
	conn, err := grpc.NewClient(*baseAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Impossibile inizializzare il client gRPC (%s): %v", *baseAddr, err)
	}
	defer func(c *grpc.ClientConn) { _ = c.Close() }(conn)

	client := generatorpb.NewEventGeneratorClient(conn)

	// TEST DI CONNETTIVITÀ PREVENTIVO
	testCtx, testCancel := context.WithTimeout(ctx, 3*time.Second)
	defer testCancel()

	testReq := &generatorpb.Sensor{
		SensorId:         "TEST-PING-0000",
		Type:             "TemperatureSensor",
		MachineToControl: "machine_test",
		BaseMean:         25.0,
		Variance:         0.1,
		IntervalNano:     time.Second.Nanoseconds(),
		SogliaMinima:     0.0,
		SogliaMassima:    100.0,
	}

	log.Println("[CHECK] Verifica connettività gRPC col cluster...")
	_, err = client.InsertNewSensor(testCtx, testReq)
	if err != nil {
		log.Fatalf("[ERRORE CRITICO] Il server gRPC non risponde su %s.\n"+
			"-> Verificare che lo IP sia corretto.\n"+
			"-> Verificare che la porta 30051 sia aperta nel Security Group di AWS.\n"+
			"Dettaglio Errore: %v", *baseAddr, err)
	}
	log.Println("[OK] Connessione gRPC verificata con successo!")

	fmt.Printf("\n=== DEMO LIVE sensori su %s ===\n\n", *baseAddr)

	stages := []stageDemo{
		{name: "Aggiunta Sensori (+250 sensori)", sensorCount: 250, interval: 1 * time.Second, holdTime: 20 * time.Second, concurrency: 4},
		{name: "Aggiunta Sensori (+250 sensori)", sensorCount: 250, interval: 2 * time.Second, holdTime: 120 * time.Second, concurrency: 4},
	}

	var totalCreated int64
	var totalSuccess, totalFailed int64

	for _, st := range stages {
		select {
		case <-ctx.Done():
			printSummary(totalSuccess, totalFailed, totalCreated)
			return
		default:
		}

		log.Printf("\n--- %s ---", st.name)
		log.Printf("-> Aggiunta di %d sensori (concorrenza: %d, intervallo: %v)...",
			st.sensorCount, st.concurrency, st.interval)

		success, failed := addSensors(ctx, client, st, &totalCreated)
		totalSuccess += success
		totalFailed += failed

		log.Printf("-> Fase completata: %d creati, %d falliti. Sensori totali attivi: %d",
			success, failed, totalCreated)
		log.Printf("-> Mantenimento carico per %v: osserva ora l'HPA scalare...", st.holdTime)

		select {
		case <-time.After(st.holdTime):
		case <-ctx.Done():
			printSummary(totalSuccess, totalFailed, totalCreated)
			return
		}
	}

	printSummary(totalSuccess, totalFailed, totalCreated)
	log.Println("\n[INFO] Demo di carico terminata. Il carico resta attivo: osserva ora lo scale-down")
	log.Println("       dello HPA nei prossimi 2-3 minuti (dipende da stabilizationWindowSeconds).")
}

func addSensors(ctx context.Context, client generatorpb.EventGeneratorClient, st stageDemo, totalCreated *int64) (success, failed int64) {
	jobs := make(chan int, st.sensorCount)
	var wg sync.WaitGroup
	var s, f int64
	var printErrOnce sync.Once

	for w := 0; w < st.concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				id := atomic.AddInt64(totalCreated, 1)
				sensorType := "TemperatureSensor"
				if j%2 == 0 {
					sensorType = "PressureSensor"
				}

				req := &generatorpb.Sensor{
					SensorId:         fmt.Sprintf("DEMO-SENS-%04d", id),
					Type:             sensorType,
					MachineToControl: fmt.Sprintf("machine_demo_%d", id%10),
					BaseMean:         float32(20 + rand.Intn(50)),
					Variance:         0.25,
					IntervalNano:     st.interval.Nanoseconds(),
					SogliaMinima:     0.0,
					SogliaMassima:    100.0,
					MaxStdDev:        2.0,
					MaxDrift:         0.05,
				}

				callCtx, cancelCall := context.WithTimeout(ctx, 5*time.Second)
				_, err := client.InsertNewSensor(callCtx, req)
				cancelCall()

				if err != nil {
					atomic.AddInt64(&f, 1)
					printErrOnce.Do(func() {
						log.Printf("  [PRIMO ERRORE DETTAGLIATO] Sensore %s: %v", req.SensorId, err)
					})
					continue
				}
				atomic.AddInt64(&s, 1)
			}
		}()
	}

	for j := 1; j <= st.sensorCount; j++ {
		jobs <- j
	}
	close(jobs)
	wg.Wait()

	return s, f
}

func printSummary(success, failed, totalCreated int64) {
	fmt.Println("\n=== RIEPILOGO DEMO ===")
	fmt.Printf("Sensori creati con successo: %d\n", success)
	fmt.Printf("Sensori falliti: %d\n", failed)
	fmt.Printf("Sensori totali attivi nel sistema: %d\n", totalCreated)
}
