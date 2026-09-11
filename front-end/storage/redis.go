package storage

import (
	"context"
	"fmt"
	"time"

	"progettoSDCC/front-end/config"

	"github.com/redis/go-redis/v9"
)

func NewRedisWriter(payload config.RedisParameter) (*redis.Client, error) {
	conn := redis.NewClient(&redis.Options{
		Addr:     payload.Address,
		Password: payload.Password,
		DB:       payload.DefaultDB,

		// La libreria gestisce già i retry automatici sulle operazioni
		MaxRetries:      5,
		MinRetryBackoff: 500 * time.Millisecond,
		MaxRetryBackoff: 3 * time.Second,

		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
		MinIdleConns: 5,
	})

	initCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := pingWithRetry(initCtx, conn, 5, 1*time.Second); err != nil {
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
