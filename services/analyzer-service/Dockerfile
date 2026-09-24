# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia tutto il progetto per soddisfare go.work e i moduli collegati
COPY . .

# Entra nella cartella specifica dell'analyzer-service
WORKDIR /app/services/analyzer-service

# Compila l'eseguibile
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o analyzer-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato
COPY --from=builder /app/services/analyzer-service/analyzer-service .

ENTRYPOINT ["./analyzer-service"]