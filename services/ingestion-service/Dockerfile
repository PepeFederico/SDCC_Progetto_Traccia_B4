# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia i file di dipendenza Go dalla radice del progetto
COPY go.mod go.sum ./
RUN go mod download

# Copia tutto il codice sorgente del progetto
COPY . .

# Compila cercando il main.go nella directory corrente
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o ingestion-service .

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/ingestion-service .

CMD ["./ingestion-service"]