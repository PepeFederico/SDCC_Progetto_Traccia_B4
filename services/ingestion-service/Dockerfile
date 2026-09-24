# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia tutto il progetto per soddisfare go.work e i moduli collegati
COPY . .

# Entra nella cartella specifica dell'ingestion-service
WORKDIR /app/services/ingestion-service

# Compila l'eseguibile
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o ingestion-service .

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/services/ingestion-service/ingestion-service .

CMD ["./ingestion-service"]