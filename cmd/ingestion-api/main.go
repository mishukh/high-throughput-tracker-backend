package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mishukh/fleet-tracker/internal/domain"
	"github.com/mishukh/fleet-tracker/internal/kafka"
)

func main() {
	kafkaBrokers := []string{getEnv("KAFKA_BROKERS", "localhost:19092")}
	kafkaTopic := getEnv("KAFKA_TOPIC", "telemetry")
	port := getEnv("PORT", "8080")

	producer := kafka.NewProducer(kafkaBrokers, kafkaTopic)
	defer producer.Close()

	r := gin.Default()

	r.POST("/api/v1/telemetry", func(c *gin.Context) {
		var req struct {
			AssetID   string  `json:"asset_id" binding:"required"`
			Latitude  float64 `json:"latitude" binding:"required"`
			Longitude float64 `json:"longitude" binding:"required"`
		}

		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		telemetry := domain.Telemetry{
			AssetID:   req.AssetID,
			Latitude:  req.Latitude,
			Longitude: req.Longitude,
			Timestamp: time.Now().UTC(),
		}

		if err := producer.Produce(c.Request.Context(), telemetry); err != nil {
			log.Printf("Failed to produce: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Internal Server Error"})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{"status": "accepted"})
	})

	log.Printf("Starting ingestion API on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
