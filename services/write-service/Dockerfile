# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia tutto il progetto per soddisfare go.work e i moduli collegati
COPY . .

# Entra nella cartella specifica di write-service
WORKDIR /app/services/write-service

# Compila l'eseguibile statico (CGO disabilitato)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o write-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/services/write-service/write-service .

ENTRYPOINT ["./write-service"]