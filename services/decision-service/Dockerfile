# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia l'intero progetto per permettere la risoluzione delle dipendenze condivise (prometheus, kafka, ecc.)
COPY . .

# Compila posizionandoti nella cartella del singolo servizio
WORKDIR /app/services/decision-service
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o decision-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo stage builder
COPY --from=builder /app/services/decision-service/decision-service .

EXPOSE 2112

ENTRYPOINT ["./decision-service"]