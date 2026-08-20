# ==========================================
# STAGE 1: Build dell'applicazione
# ==========================================
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copia i file di dipendenza Go
COPY go.mod go.sum ./
RUN go mod download

# Copia tutto il codice sorgente
COPY . .

# Compila il binario dentro la cartella dove si trova main.go
# Nota: //go:embed incorporerà automaticamente web-page dentro l'eseguibile!
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o events-generator ./events-generator

# ==========================================
# STAGE 2: Immagine di runtime minimale
# ==========================================
FROM alpine:latest

WORKDIR /root/

# Copia SOLO il binario (contiene già la web-page grazie a //go:embed)
COPY --from=builder /app/events-generator .

EXPOSE 8081

CMD ["./events-generator"]