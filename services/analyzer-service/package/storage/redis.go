package storage

import (
	"analyzer-service/package/config"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedisWriter(payload config.RedisParameter) (*redis.Client, error) {
	//	Creazione nuova connessione Redis
	conn := redis.NewClient(&redis.Options{
		Addr:     payload.Address,
		Password: payload.Password,
		DB:       payload.DefaultDB,

		//  Gestione dei Retry
		MaxRetries:      5,                      //  Riprova fino a un massimo di 5 volte, prima di restituire l'errore
		MinRetryBackoff: 500 * time.Millisecond, //  Attesa minima tra i due tentativi
		MaxRetryBackoff: 3 * time.Second,        //  Attesa massima tra i due tentativi

		//  Gestione pool e timeout TCP
		DialTimeout:  5 * time.Second, // Timeout per stabilire la connessione TCP
		ReadTimeout:  3 * time.Second, // Timeout per la lettura della risposta
		WriteTimeout: 3 * time.Second, // Timeout per la scrittura del comando
		PoolSize:     20,              // Connessioni TCP massime aperte
		MinIdleConns: 5,               // Connessioni TCP tenute sempre pronte
	})

	initCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := pingWithRetry(initCtx, conn, 5, 2*time.Second); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("impossibile connettersi a Redis: %w", err)
	}

	fmt.Println("Connessione Redis stabilita con successo.")
	return conn, nil
}

func pingWithRetry(ctx context.Context, conn *redis.Client, maxAttempts int, delay time.Duration) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = conn.Ping(ctx).Err()
		if err == nil {
			return nil
		}

		fmt.Printf("Tentativo Ping %d/%d fallito: %v. Riprovo tra %v...\n", attempt, maxAttempts, err, delay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return err
}

func SaveSnapshot(ctx context.Context, snapshot map[string]config.SensorState, conn *redis.Client) error {
	if len(snapshot) == 0 {
		return nil
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("errore marshal snapshot: %w", err)
	}

	if err := conn.Set(ctx, "checkpoint:sensor-windows", data, 0).Err(); err != nil {
		return fmt.Errorf("errore salvataggio snapshot su Redis: %w", err)
	}

	return nil
}

func LoadSnapshot(ctx context.Context, conn *redis.Client) (map[string]config.SensorState, error) {
	val, err := conn.Get(ctx, "checkpoint:sensor-windows").Result()
	if err != nil {
		// Gestione del caso in cui la chiave non esista ancora (primo avvio assoluto)
		if errors.Is(err, redis.Nil) {

			return make(map[string]config.SensorState), nil
		}
		return nil, err
	}

	var snapshot map[string]config.SensorState
	if err := json.Unmarshal([]byte(val), &snapshot); err != nil {
		return nil, fmt.Errorf("errore unmarshal snapshot: %w", err)
	}

	return snapshot, nil
}

func LoadTimestamp(ctx context.Context, sensorID string, conn *redis.Client) (time.Time, error) {
	val, err := conn.Get(ctx, fmt.Sprintf("sensor:stopped_at:%s", sensorID)).Result()
	if err != nil {
		return time.Time{}, err
	}

	timeStamp, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, fmt.Errorf("errore parsing timestamp RFC3339: %w", err)
	}

	return timeStamp, nil
}
