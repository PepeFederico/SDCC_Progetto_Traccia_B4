# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copia l'intero contenuto della root nel container
# In questo modo go.work troverà tutte le sottocartelle dei moduli
COPY . .

# Entra nella cartella del servizio
WORKDIR /app/events-generator

# Compila il binario (Go gestirà automaticamente le dipendenze via go.work)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o events-generator .

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia solo il binario compilato
COPY --from=builder /app/events-generator/events-generator .

CMD ["./events-generator"]