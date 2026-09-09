# ==========================================
# STAGE 1: Build
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia i file del modulo dalla radice
COPY go.mod go.sum ./
RUN go mod download

# Copia l'intero progetto
COPY . .

# Compila specificando la sottocartella dove risiede il codice di sink-service
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o sink-service ./services/sink-service

# ==========================================
# STAGE 2: Runtime
# ==========================================
FROM alpine:latest

WORKDIR /root/

COPY --from=builder /app/sink-service .

ENTRYPOINT ["./sink-service"]