package domain

import "context"

type TelemetryProducer interface {
	Produce(ctx context.Context, t Telemetry) error
}

type TelemetryCache interface {
	SetLatestLocation(ctx context.Context, t Telemetry) error
	GetLatestLocation(ctx context.Context, assetID string) (*Telemetry, error)
	PublishLocation(ctx context.Context, t Telemetry) error
}

type TelemetryStorage interface {
	InsertBatch(ctx context.Context, telemetries []Telemetry) error
	GetRouteHistory(ctx context.Context, assetID string, start, end string) ([]Telemetry, error)
}
