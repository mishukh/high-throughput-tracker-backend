package domain

import "time"

type Asset struct {
	ID         string    `json:"id"`
	DriverName string    `json:"driver_name"`
	CreatedAt  time.Time `json:"created_at"`
}

type Telemetry struct {
	AssetID   string    `json:"asset_id"`
	Latitude  float64   `json:"latitude"`
	Longitude float64   `json:"longitude"`
	Timestamp time.Time `json:"timestamp"`
}
