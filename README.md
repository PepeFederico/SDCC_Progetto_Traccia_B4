# Progetto SDCC -- Traccia B4 -- Applicazioen a microservizi -- A.A. 2025/2026

Sistema IoT a microservizi per l'ingestion, l'elaborazione e l'analisi in tempo reale di dati sensoristici, basato su un'architettura event-driven con Apache Kafka, Redis, InfluxDB e uno stack di monitoraggio Prometheus/Grafana.

## Architettura

Il sistema è composto dai seguenti servizi, orchestrati tramite Docker Compose:

| Servizio | Ruolo |
|---|---|
| `kafka` | Broker di messaggistica (modalità KRaft, single-node) |
| `events-generator` | Simulatore di sensori IoT, espone un'API gRPC |
| `front-end` | API Gateway verso generatore e sink-service |
| `ingestion-service` | Consuma i dati grezzi dai topic Kafka |
| `analyzer-service` | Calcola metriche aggregate su finestre temporali |
| `decision-service` | Rilevamento anomalie sui dati aggregati |
| `sink-service` | Scrive i risultati elaborati su InfluxDB |
| `write-service` | Scrive i dati grezzi/telemetria su InfluxDB |
| `redis` | Cache dei parametri di configurazione sensore |
| `influxdb` | Database time-series per dati grezzi e aggregati |
| `prometheus` / `grafana` | Raccolta e visualizzazione metriche |
| `cadvisor` | Metriche di utilizzo risorse per container |
| `kafka-exporter` | Espone metriche Kafka in formato Prometheus |

## Requisiti

### Software

- **Docker Engine** 24.x o superiore
- **Docker Compose** v2 (plugin `docker compose`, non il vecchio `docker-compose` standalone)
- **Go 1.25.0** — richiesto solo per sviluppo locale (compilazione manuale, rigenerazione dei file `.proto`) o per l'esecuzione degli script di test di carico; non necessario per il solo avvio via Docker Compose, poiché la build dei microservizi avviene interamente nei container
- **Protocol Buffers Compiler (`protoc`)** e i plugin Go — richiesti solo se si modificano i file `.proto` e serve rigenerare il codice (vedi sezione dedicata più sotto)
- **Sistema operativo host: Linux** — il servizio `cadvisor` monta percorsi specifici del filesystem Docker su Linux (`/var/lib/docker`, `/var/run`, `/sys`). Su **Windows/macOS con Docker Desktop** questi percorsi non sono accessibili allo stesso modo dalla VM interna: `cadvisor` potrebbe avviarsi ma raccogliere metriche incomplete o nulle. Per un utilizzo su quei sistemi, valutare la rimozione del servizio `cadvisor` dal file compose o l'uso di WSL2 su Linux nativo.

### Risorse hardware consigliate

Il sistema esegue simultaneamente un broker Kafka (heap JVM da 1 GB), InfluxDB, Redis, 7 microservizi Go, Prometheus, Grafana e cAdvisor. Si consiglia di allocare a Docker almeno:
- **4 vCPU**
- **8 GB di RAM**

Con risorse inferiori, Kafka e i servizi di elaborazione possono andare incontro a rallentamenti o riavvii per pressione di memoria/CPU.

### File e directory richiesti

Prima dell'avvio, assicurarsi che esistano i seguenti file referenziati dal `docker-compose.yml` (non generati automaticamente):
- `./kafka/init-kafka.sh` — script di creazione dei topic Kafka
- `./prometheus/config/prometheus.yml` — configurazione degli scrape target di Prometheus

La directory `./redis/data` (usata per la persistenza di Redis) viene creata automaticamente da Docker se non esiste.

### Porte esposte sull'host

Verificare che le seguenti porte siano libere prima dell'avvio:

| Porta | Servizio |
|---|---|
| 9096, 9094 | Kafka (interna / listener esterno) |
| 50051 | events-generator (gRPC) |
| 8080 | front-end (API Gateway) |
| 8086 | InfluxDB |
| 6379 | Redis |
| 9090 | Prometheus |
| 3000 | Grafana |
| 8081 | cAdvisor |
| 9308 | kafka-exporter |

## Avvio del sistema

```bash
docker compose up -d --build
```

Il primo avvio può richiedere alcuni minuti per la build delle immagini dei microservizi Go e l'inizializzazione di Kafka/InfluxDB. I servizi `init-kafka` e `init-influxdb` attendono automaticamente che i rispettivi backend siano pronti (`service_healthy`) prima di creare topic e bucket.

Per verificare lo stato di tutti i servizi:
```bash
docker compose ps
```

Per arrestare e rimuovere tutto:
```bash
docker compose down
```

## Punti di accesso e credenziali di default

| Servizio | URL | Credenziali |
|---|---|---|
| Front-end / Dashboard | http://localhost:8080 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| InfluxDB UI | http://localhost:8086 | `admin` / `adminpassword123` |
| Redis | `localhost:6379` | password: `qwertyuiopo1` |

**InfluxDB — organizzazione e bucket:**
- Organizzazione: `sdcc_org`
- Bucket: `telemetry` (dati grezzi, usato da `write-service`), `measurement` (dati aggregati, usato da `sink-service`)
- Token amministrativo: `my-super-secret-token`

## Generazione del codice dai file `.proto`

Il progetto usa gRPC per la comunicazione tra `front-end`/`events-generator` (e potenzialmente altri servizi). Il codice Go generato dai file `.proto` **è già incluso nel repository**: la rigenerazione è necessaria solo se si modifica una definizione `.proto` esistente o se ne aggiunge una nuova.

### Installazione degli strumenti richiesti (una tantum)

```bash
# Compilatore Protocol Buffers
# Debian/Ubuntu:
sudo apt install -y protobuf-compiler
# macOS:
brew install protobuf

# Verifica versione (consigliata >= 3.21)
protoc --version

# Plugin Go per protoc
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Assicurarsi che $GOPATH/bin sia nel PATH, altrimenti protoc non trova i plugin
export PATH="$PATH:$(go env GOPATH)/bin"
```

### Rigenerazione del codice

Dalla root del progetto, per ogni file `.proto` modificato:

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/event-generator/event_generator.proto
```

Questo rigenera nella stessa directory del `.proto` sia `event_generator.pb.go` (i messaggi) sia `event_generator_grpc.pb.go` (i client/server gRPC). Ripetere il comando, adattando il percorso, per ogni altro file `.proto` presente nel progetto (es. se `sink-service` ha una propria definizione gRPC).

> Dopo la rigenerazione, ricompilare i servizi che dipendono dai messaggi modificati:
> ```bash
> docker compose build events-generator front-end ingestion-service
> ```

## Note

- **Persistenza Redis disabilitata di default** (`--save "" --appendonly no`): i dati in Redis (parametri di configurazione sensore) vengono persi ad ogni riavvio del container. Il servizio `events-generator` li ripopola al proprio avvio.
- Tutti i servizi Go (`events-generator`, `front-end`, `ingestion-service`, `analyzer-service`, `decision-service`, `sink-service`, `write-service`) vengono buildati localmente dai rispettivi `Dockerfile`: non è richiesta un'installazione di Go sull'host, la build avviene interamente all'interno di Docker.
