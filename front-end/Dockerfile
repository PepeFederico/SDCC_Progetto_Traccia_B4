# ==========================================
# STAGE 1: Build dell'applicazione Go
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# 1. Copia i file del modulo dalla root del progetto
COPY go.mod go.sum ./
RUN go mod download

# 2. Copia tutto il codice sorgente
COPY . .

# 3. Compila il binario dell'API Gateway indicando la sotto-cartella
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o api-gateway ./front-end/main.go

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# 1. Copia il binario compilato dallo STAGE 1
COPY --from=builder /app/api-gateway .

# 2. Copia la cartella web-page dal percorso corretto del builder
COPY --from=builder /app/front-end/web-page ./web-page

EXPOSE 8080

CMD ["./api-gateway"]