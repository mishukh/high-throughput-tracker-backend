package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
	"github.com/mishukh/fleet-tracker/internal/postgres"
	"github.com/mishukh/fleet-tracker/internal/redis"
)

func getNginxMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})
	resp, err := http.Get("http://api-gateway/nginx_status")
	if err == nil {
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		str := string(body)
		
		lines := strings.Split(str, "\n")
		if len(lines) >= 4 {
			metrics["active_connections"] = strings.TrimSpace(strings.Split(lines[0], ":")[1])
			
			counts := strings.Fields(lines[2])
			if len(counts) >= 3 {
				metrics["accepted_connections"] = counts[0]
				metrics["handled_connections"] = counts[1]
				metrics["total_requests_routed"] = counts[2]
			}
			
			metrics["current_state"] = strings.TrimSpace(lines[3])
		}
	} else {
		metrics["status"] = "offline or unreachable"
	}
	return metrics
}

func getKafkaMetrics(broker string, topic string) map[string]interface{} {
	metrics := make(map[string]interface{})
	metrics["broker"] = broker
	metrics["topic"] = topic
	metrics["status"] = "offline"

	conn, err := kafka.Dial("tcp", broker)
	if err == nil {
		defer conn.Close()
		metrics["status"] = "online"
		partitions, err := conn.ReadPartitions(topic)
		if err == nil {
			metrics["total_partitions"] = len(partitions)
			if len(partitions) > 0 {
				leaderConn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, partitions[0].ID)
				if err == nil {
					defer leaderConn.Close()
					first, last, _ := leaderConn.ReadOffsets()
					metrics["partition_0_oldest_offset"] = first
					metrics["partition_0_latest_offset"] = last
					metrics["partition_0_total_messages_ingested"] = last - first
				}
			}
		}
	}
	return metrics
}

func main() {
	port := getEnv("PORT", "8081")
	redisAddr := getEnv("REDIS_ADDR", "")
	dbConn := getEnv("DB_CONN", "")

	cache := redis.NewCache(redisAddr)
	defer cache.Close()

	storage, err := postgres.NewStorage(context.Background(), dbConn)
	if err != nil {
		log.Fatalf("Failed to init postgres: %v", err)
	}
	defer storage.Close()

	r := gin.Default()

	r.GET("/api/v1/assets/:id/location", func(c *gin.Context) {
		assetID := c.Param("id")
		
		loc, err := cache.GetLatestLocation(c.Request.Context(), assetID)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if loc == nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "asset not found or no location reported"})
			return
		}

		c.JSON(http.StatusOK, loc)
	})

	r.GET("/api/v1/assets/:id/route", func(c *gin.Context) {
		assetID := c.Param("id")
		start := c.Query("start")
		end := c.Query("end")

		if start == "" || end == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "start and end timestamps are required"})
			return
		}

		history, err := storage.GetRouteHistory(c.Request.Context(), assetID, start, end)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, history)
	})

	r.GET("/api/v1/system/metrics", func(c *gin.Context) {
		ctx := c.Request.Context()
		
		dbMetrics, err := storage.GetDBMetrics(ctx)
		if err != nil {
			dbMetrics = map[string]interface{}{"error": err.Error()}
		}

		cacheMetrics, err := cache.GetCacheMetrics(ctx)
		if err != nil {
			cacheMetrics = map[string]interface{}{"error": err.Error()}
		}

		c.JSON(http.StatusOK, gin.H{
			"status": "healthy",
			"database_timescaledb": dbMetrics,
			"cache_redis": cacheMetrics,
			"queue_kafka": getKafkaMetrics("redpanda:9092", "telemetry"),
			"gateway_nginx": getNginxMetrics(),
		})
	})

	log.Printf("Starting query API on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
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
