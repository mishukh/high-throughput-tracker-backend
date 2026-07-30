package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

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
		MaxWait:  50 * time.Millisecond,
	})
	return &Consumer{reader: r}
}

func (c *Consumer) ConsumeBatch(ctx context.Context, batchSize int, timeout time.Duration, handler func(context.Context, []domain.Telemetry) error) {
	for {
		var batch []domain.Telemetry
		var messages []kafka.Message

		timeoutCtx, cancel := context.WithTimeout(ctx, timeout)

		for len(batch) < batchSize {
			m, err := c.reader.FetchMessage(timeoutCtx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) && len(batch) > 0 {
					break // Timeout hit, process what we have
				}
				if errors.Is(err, context.Canceled) {
					cancel()
					return
				}
				continue
			}

			var t domain.Telemetry
			if err := json.Unmarshal(m.Value, &t); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				c.reader.CommitMessages(ctx, m) // skip bad messages
				continue
			}

			batch = append(batch, t)
			messages = append(messages, m)
		}
		cancel()

		if len(batch) > 0 {
			if err := handler(ctx, batch); err != nil {
				log.Printf("Handler failed: %v", err)
				continue
			}

			if err := c.reader.CommitMessages(ctx, messages...); err != nil {
				log.Printf("Failed to commit messages: %v", err)
			}
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
