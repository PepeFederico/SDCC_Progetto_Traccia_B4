# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copia i file di dipendenza per sfruttare la cache
COPY go.mod go.sum ./
RUN go mod download

# Copia tutto il codice sorgente
COPY . .

# Compila l'eseguibile statico (CGO disabilitato)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o sink-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato
COPY --from=builder /app/sink-service .

ENTRYPOINT ["./sink-service"]