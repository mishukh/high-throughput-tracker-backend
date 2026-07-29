package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/mishukh/fleet-tracker/internal/domain"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	return &Consumer{reader: r}
}

func (c *Consumer) Consume(ctx context.Context, handler func(context.Context, domain.Telemetry) error) {
	for {
		m, err := c.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Failed to fetch message: %v", err)
			continue
		}

		var t domain.Telemetry
		if err := json.Unmarshal(m.Value, &t); err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			c.reader.CommitMessages(ctx, m) // skip bad messages
			continue
		}

		if err := handler(ctx, t); err != nil {
			log.Printf("Handler failed: %v", err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, m); err != nil {
			log.Printf("Failed to commit message: %v", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
