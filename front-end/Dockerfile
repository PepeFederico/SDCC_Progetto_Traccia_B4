# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia l'intero progetto dalla root per soddisfare go.work
COPY . .

# Entra nella cartella del front-end
WORKDIR /app/front-end

# Compila l'eseguibile api-gateway
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o api-gateway .

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/front-end/api-gateway .

# Copia la cartella web-page necessaria al front-end
COPY --from=builder /app/front-end/web-page ./web-page

EXPOSE 8080

CMD ["./api-gateway"]