package main

import (
	"context"
	"log"
	"os"
	"time"

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

	consumer.ConsumeBatch(ctx, 1000, 1*time.Second, func(ctx context.Context, batch []domain.Telemetry) error {
		log.Printf("Processing batch of %d telemetry records", len(batch))

		if err := storage.InsertBatch(ctx, batch); err != nil {
			log.Printf("Error updating postgres: %v", err)
			return err
		}

		for _, t := range batch {
			if err := cache.SetLatestLocation(ctx, t); err != nil {
				log.Printf("Error updating redis for asset %s: %v", t.AssetID, err)
			}
			if err := cache.PublishLocation(ctx, t); err != nil {
				log.Printf("Error publishing to redis for asset %s: %v", t.AssetID, err)
			}
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
