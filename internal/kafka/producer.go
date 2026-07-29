package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/mishukh/fleet-tracker/internal/domain"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	return &Producer{writer: w}
}

func (p *Producer) Produce(ctx context.Context, t domain.Telemetry) error {
	bytes, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("failed to marshal telemetry: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(t.AssetID), // Partition by asset ID to ensure ordering per asset
		Value: bytes,
	}

	err = p.writer.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("failed to write message to kafka: %w", err)
	}

	log.Printf("Produced telemetry for asset %s", t.AssetID)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
