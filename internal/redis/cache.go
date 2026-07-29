package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/mishukh/fleet-tracker/internal/domain"
)

type Cache struct {
	client *redis.Client
}

func NewCache(addr string) *Cache {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &Cache{client: rdb}
}

func (c *Cache) SetLatestLocation(ctx context.Context, t domain.Telemetry) error {
	key := fmt.Sprintf("asset:%s:latest", t.AssetID)
	bytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	err = c.client.Set(ctx, key, bytes, 24*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("redis set failed: %w", err)
	}
	return nil
}

func (c *Cache) GetLatestLocation(ctx context.Context, assetID string) (*domain.Telemetry, error) {
	key := fmt.Sprintf("asset:%s:latest", assetID)
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil // not found
	} else if err != nil {
		return nil, err
	}

	var t domain.Telemetry
	if err := json.Unmarshal([]byte(val), &t); err != nil {
		return nil, err
	}

	return &t, nil
}

func (c *Cache) PublishLocation(ctx context.Context, t domain.Telemetry) error {
	channel := "telemetry_updates"
	bytes, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return c.client.Publish(ctx, channel, bytes).Err()
}

func (c *Cache) GetCacheMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	
	info, err := c.client.Info(ctx, "memory", "clients", "stats").Result()
	if err == nil {
		lines := strings.Split(info, "\r\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "used_memory_human:") {
				metrics["memory_used"] = strings.TrimPrefix(line, "used_memory_human:")
			}
			if strings.HasPrefix(line, "connected_clients:") {
				metrics["connected_clients"] = strings.TrimPrefix(line, "connected_clients:")
			}
			if strings.HasPrefix(line, "instantaneous_ops_per_sec:") {
				metrics["ops_per_second"] = strings.TrimPrefix(line, "instantaneous_ops_per_sec:")
			}
			if strings.HasPrefix(line, "total_connections_received:") {
				metrics["lifetime_connections"] = strings.TrimPrefix(line, "total_connections_received:")
			}
		}
	}

	dbSize, err := c.client.DBSize(ctx).Result()
	if err == nil {
		metrics["total_active_assets"] = dbSize
	}

	return metrics, nil
}

func (c *Cache) Close() error {
	return c.client.Close()
}
