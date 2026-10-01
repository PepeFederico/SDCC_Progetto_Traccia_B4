package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	generatorpb "progettoSDCC/proto/event-generator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type actionType int

const (
	ActionAdd actionType = iota
	ActionStop
	ActionHold
)

type stage struct {
	name        string
	action      actionType    // ActionAdd, ActionStop, o ActionHold
	sensorCount int           // Numero di sensori da aggiungere/stoppare
	interval    time.Duration // Frequenza di invio dati del sensore (es. 500ms, 1s, 2s)
	holdTime    time.Duration // Tempo di mantenimento della fase
	concurrency int           // Goroutine in parallelo per l'invio delle RPC gRPC
}

type statsExt struct {
	success        int64
	failed         int64
	totalLatencyMs int64
	latenciesMs    []int64
	mu             sync.Mutex
}

func (s *statsExt) AddSuccess(latency int64) {
	atomic.AddInt64(&s.success, 1)
	atomic.AddInt64(&s.totalLatencyMs, latency)
	s.mu.Lock()
	s.latenciesMs = append(s.latenciesMs, latency)
	s.mu.Unlock()
}

func (s *statsExt) AddFailed() {
	atomic.AddInt64(&s.failed, 1)
}

func (s *statsExt) PrintPercentiles() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latenciesMs) == 0 {
		log.Println("   [LATENZE gRPC] Nessuna operazione completata in questo stage.")
		return
	}

	sort.Slice(s.latenciesMs, func(i, j int) bool { return s.latenciesMs[i] < s.latenciesMs[j] })

	count := len(s.latenciesMs)
	p50 := s.latenciesMs[int(float64(count)*0.50)]
	p95 := s.latenciesMs[int(float64(count)*0.95)]
	p99 := s.latenciesMs[int(float64(count)*0.99)]
	m := s.latenciesMs[count-1]

	log.Printf("   [LATENZE gRPC CREAZIONE] P50: %dms | P95: %dms | P99: %dms | Max: %dms (su %d ops)",
		p50, p95, p99, m, count)
}

type sensorGroup struct {
	count    int
	interval time.Duration
}

func estimatedTotalRate(groups []sensorGroup) float64 {
	rate := 0.0
	for _, g := range groups {
		if g.interval > 0 {
			rate += float64(g.count) / g.interval.Seconds()
		}
	}
	return rate
}

func main() {
	baseAddr := flag.String("addr", "localhost:50051", "Indirizzo gRPC del generatore (es. IP:Porta o ClusterIP:Porta)")
	numConns := flag.Int("conns", 8, "Numero di connessioni gRPC esterne nel pool")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("\n[STOP] Interruzione del test richiesta, arresto in corso...")
		cancel()
	}()

	clients := make([]generatorpb.EventGeneratorClient, *numConns)
	for i := 0; i < *numConns; i++ {
		conn, err := grpc.NewClient(*baseAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Impossibile connettersi al generatore (%s): %v", *baseAddr, err)
		}
		defer func(c *grpc.ClientConn) { _ = c.Close() }(conn)
		clients[i] = generatorpb.NewEventGeneratorClient(conn)
	}

	fmt.Printf("=== INIZIO STRESS TEST REALISTICO (RAMP-UP 10K + CHAOS RECOVERY) su %s ===\n", *baseAddr)

	// PIANIFICAZIONE TEST:
	stages := []stage{
		// --- RAMPA INIZIALE FINO A 3.000 SENSORI ---
		{name: "Fase 1: Riferimento iniziale (50 sensori @ 1s)", action: ActionAdd, sensorCount: 50, interval: 1 * time.Second, holdTime: 20 * time.Second, concurrency: 4},
		{name: "Fase 2: Warm-up (200 sensori @ 1s)", action: ActionAdd, sensorCount: 150, interval: 1 * time.Second, holdTime: 20 * time.Second, concurrency: 8},
		{name: "Fase 3: Carico Moderato (500 sensori @ 500ms)", action: ActionAdd, sensorCount: 300, interval: 500 * time.Millisecond, holdTime: 30 * time.Second, concurrency: 16},
		{name: "Fase 4: Carico Elevato (1.5k sensori @ 500ms)", action: ActionAdd, sensorCount: 1000, interval: 500 * time.Millisecond, holdTime: 45 * time.Second, concurrency: 24},
		{name: "Fase 5: 1° Soglia Obiettivo (3k sensori @ 1s)", action: ActionAdd, sensorCount: 1500, interval: 1 * time.Second, holdTime: 60 * time.Second, concurrency: 32},

		// --- ARRESTO MASSIVO (Stoppa 2.000 sensori sui 3.000 attivi) ---
		{name: "Fase 6: Arresto Massivo (Stop 2k sensori per testare Scale-Down)", action: ActionStop, sensorCount: 2000, holdTime: 15 * time.Second, concurrency: 32},

		// --- OSSERVAZIONE SCALE-DOWN ---
		{name: "Fase 7: Osservazione del Sistema", action: ActionHold, holdTime: 200 * time.Second},

		// --- SECONDO PICCO FINO A 10.000 SENSORI ---
		{name: "Fase 8: Carico Massivo (5k sensori @ 500ms)", action: ActionAdd, sensorCount: 4000, interval: 500 * time.Millisecond, holdTime: 60 * time.Second, concurrency: 48},
		{name: "Fase 9: Avvicinamento al Picco (7.5k sensori @ 500ms)", action: ActionAdd, sensorCount: 2500, interval: 500 * time.Millisecond, holdTime: 60 * time.Second, concurrency: 64},
		{name: "Fase 10: 2°Soglia Obiettivo (10k sensori @ 500ms)", action: ActionAdd, sensorCount: 2500, interval: 500 * time.Millisecond, holdTime: 120 * time.Second, concurrency: 80},

		// --- ARRESTO FINALE SOTTO PICCO ---
		{name: "Fase 11: Secondo Arresto Massivo (5k sensori)", action: ActionStop, sensorCount: 5000, holdTime: 30 * time.Second, concurrency: 48},
		{name: "Fase 12: Attesa Recovery Finale", action: ActionHold, holdTime: 180 * time.Second},
	}

	var totalCreated int64 = 0
	var createdIDs []string
	var idsLock sync.Mutex
	var activeGroups []sensorGroup

	overall := &statsExt{}

	for i, st := range stages {
		select {
		case <-ctx.Done():
			printFinalStats(overall, totalCreated)
			return
		default:
		}

		log.Printf("\n--- [%d/%d] %s ---", i+1, len(stages), st.name)

		stageStats := &statsExt{}

		switch st.action {
		case ActionAdd:
			log.Printf("-> Inserimento di %d nuovi sensori (Concorrenza: %d | Intervallo invio dati: %v)...",
				st.sensorCount, st.concurrency, st.interval)

			runAddStage(ctx, clients, st, &totalCreated, &createdIDs, &idsLock, stageStats)
			activeGroups = append(activeGroups, sensorGroup{count: int(stageStats.success), interval: st.interval})

		case ActionStop:
			log.Printf("-> INIEZIONE GUASTO: Invio comando di STOP a %d sensori attivi...", st.sensorCount)
			runStopStage(ctx, clients, st, &createdIDs, &idsLock, stageStats)

		case ActionHold:
			log.Printf("-> OSSERVAZIONE: Nessun nuovo comando. In attesa del riavvio autonomo dei sensori...")
		}

		if st.action != ActionHold {
			stageStats.PrintPercentiles()
			overall.success += stageStats.success
			overall.failed += stageStats.failed
			overall.totalLatencyMs += stageStats.totalLatencyMs
		}

		totalMsgRate := estimatedTotalRate(activeGroups)
		log.Printf("-> Sensori totali attivati: %d | Throughput stimato generato verso Kafka: ~%.0f msg/s", totalCreated, totalMsgRate)
		log.Printf("-> Mantenimento della fase per %v. Monitora HPA e Lag Kafka su Grafana...", st.holdTime)

		select {
		case <-time.After(st.holdTime):
		case <-ctx.Done():
			printFinalStats(overall, totalCreated)
			return
		}
	}

	printFinalStats(overall, totalCreated)
}

func runAddStage(ctx context.Context, clients []generatorpb.EventGeneratorClient, st stage, totalCreated *int64, createdIDs *[]string, idsLock *sync.Mutex, stats *statsExt) {
	jobs := make(chan int, st.sensorCount)
	var wg sync.WaitGroup

	for w := 0; w < st.concurrency; w++ {
		wg.Add(1)
		client := clients[w%len(clients)]

		go func(c generatorpb.EventGeneratorClient, workerID int) {
			defer wg.Done()
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for j := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				id := atomic.AddInt64(totalCreated, 1)
				sensorID := fmt.Sprintf("RAMP-SENS-%05d", id)

				sensorType := "TemperatureSensor"
				if j%2 == 0 {
					sensorType = "PressureSensor"
				}

				req := &generatorpb.Sensor{
					SensorId:         sensorID,
					Type:             sensorType,
					MachineToControl: fmt.Sprintf("machine_ramp_%d", id%10),
					BaseMean:         float32(20 + localRand.Intn(50)),
					Variance:         0.25,
					IntervalNano:     st.interval.Nanoseconds(), // Imposta correttamente l'intervallo (es. 500ms o 1s in nanosecondi)
					SogliaMinima:     0.0,
					SogliaMassima:    100.0,
					MaxStdDev:        2.0,
					MaxDrift:         0.05,
				}

				callCtx, cancelCall := context.WithTimeout(ctx, 5*time.Second)
				start := time.Now()
				_, err := c.InsertNewSensor(callCtx, req)
				elapsed := time.Since(start).Milliseconds()
				cancelCall()

				if err != nil {
					stats.AddFailed()
					log.Printf("  [ERRORE ADD] Sensore %s: %v", sensorID, err)
					continue
				}

				stats.AddSuccess(elapsed)

				idsLock.Lock()
				*createdIDs = append(*createdIDs, sensorID)
				idsLock.Unlock()
			}
		}(client, w)
	}

	for j := 1; j <= st.sensorCount; j++ {
		jobs <- j
	}
	close(jobs)
	wg.Wait()
}

func runStopStage(ctx context.Context, clients []generatorpb.EventGeneratorClient, st stage, createdIDs *[]string, idsLock *sync.Mutex, stats *statsExt) {
	idsLock.Lock()
	availableCount := len(*createdIDs)
	if availableCount == 0 {
		idsLock.Unlock()
		log.Println("  [WARNING] Nessun sensore disponibile da fermare.")
		return
	}

	targetCount := st.sensorCount
	if targetCount > availableCount {
		targetCount = availableCount
	}

	targets := make([]string, targetCount)
	perm := rand.Perm(availableCount)
	for i := 0; i < targetCount; i++ {
		targets[i] = (*createdIDs)[perm[i]]
	}
	idsLock.Unlock()

	jobs := make(chan string, targetCount)
	var wg sync.WaitGroup

	for w := 0; w < st.concurrency; w++ {
		wg.Add(1)
		client := clients[w%len(clients)]

		go func(c generatorpb.EventGeneratorClient) {
			defer wg.Done()

			for sensorID := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				req := &generatorpb.Mode{
					SensorId: sensorID,
					Mode:     "ModeStop",
				}

				callCtx, cancelCall := context.WithTimeout(ctx, 5*time.Second)
				start := time.Now()
				_, err := c.ChangeMode(callCtx, req)
				elapsed := time.Since(start).Milliseconds()
				cancelCall()

				if err != nil {
					stats.AddFailed()
					log.Printf("  [ERRORE STOP] Sensore %s: %v", sensorID, err)
					continue
				}

				stats.AddSuccess(elapsed)
			}
		}(client)
	}

	for _, id := range targets {
		jobs <- id
	}
	close(jobs)
	wg.Wait()
}

func printFinalStats(s *statsExt, totalCreated int64) {
	fmt.Println("\n=== STRESS TEST COMPLETO / TERMINATO ===")
	fmt.Printf("Totale registrazioni sensori riuscite via gRPC: %d\n", s.success)
	fmt.Printf("Totale registrazioni sensori fallite via gRPC: %d\n", s.failed)
	if s.success > 0 {
		fmt.Printf("Latenza gRPC media creazione sensore: %dms\n", s.totalLatencyMs/s.success)
	}
	fmt.Printf("Totale sensori attivi inseriti nel sistema: %d\n", totalCreated)
}
