package main

import (
	"context"
	"log"
	"os"

	"github.com/mishukh/fleet-tracker/internal/domain"
	"github.com/mishukh/fleet-tracker/internal/kafka"
	"github.com/mishukh/fleet-tracker/internal/postgres"
	"github.com/mishukh/fleet-tracker/internal/redis"
)

func main() {
	ctx := context.Background()

	kafkaBrokers := []string{getEnv("KAFKA_BROKERS", "localhost:19092")}
	kafkaTopic := getEnv("KAFKA_TOPIC", "telemetry")
	kafkaGroup := getEnv("KAFKA_GROUP", "stream-processor-group")
	redisAddr := getEnv("REDIS_ADDR", "")
	dbConn := getEnv("DB_CONN", "")

	cache := redis.NewCache(redisAddr)
	defer cache.Close()

	storage, err := postgres.NewStorage(ctx, dbConn)
	if err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}
	defer storage.Close()

	consumer := kafka.NewConsumer(kafkaBrokers, kafkaTopic, kafkaGroup)
	defer consumer.Close()

	log.Println("Stream Processor started. Listening for telemetry...")

	consumer.Consume(ctx, func(ctx context.Context, t domain.Telemetry) error {
		log.Printf("Processing telemetry for asset: %s", t.AssetID)

		if err := cache.SetLatestLocation(ctx, t); err != nil {
			log.Printf("Error updating redis: %v", err)
			return err
		}

		if err := storage.InsertBatch(ctx, []domain.Telemetry{t}); err != nil {
			log.Printf("Error updating postgres: %v", err)
			return err
		}

		if err := cache.PublishLocation(ctx, t); err != nil {
			log.Printf("Error publishing to redis: %v", err)
		}

		return nil
	})
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	if fallback == "" {
		log.Fatalf("Environment variable %s is securely required but missing", key)
	}
	return fallback
}
