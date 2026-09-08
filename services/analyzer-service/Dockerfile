# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia i file di dipendenza per sfruttare la cache
COPY go.mod go.sum ./
RUN go mod download

# Copia tutto il codice sorgente
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o analyzer-service .

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato
COPY --from=builder /app/analyzer-service .

ENTRYPOINT ["./analyzer-service"]