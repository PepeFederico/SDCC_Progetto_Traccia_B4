# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia l'intero progetto per soddisfare go.work e le dipendenze condivise
COPY . .

# Entra nella cartella specifica del decision-service
WORKDIR /app/services/decision-service

# Compila l'eseguibile statico
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o decision-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/services/decision-service/decision-service .

EXPOSE 2112

ENTRYPOINT ["./decision-service"]