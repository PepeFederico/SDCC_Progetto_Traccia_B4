# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# 1. Copia l'intero monorepo per permettere a go.work e go:embed di risolvere tutto
COPY . .

# 2. Scarica le dipendenze basandosi sull'intero workspace
RUN go mod download

# 3. Compila il binario dell'API Gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o api-gateway ./front-end/main.go

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# 1. Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/api-gateway .

EXPOSE 8080

CMD ["./api-gateway"]