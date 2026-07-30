package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mishukh/fleet-tracker/internal/domain"
)

type Storage struct {
	db *pgxpool.Pool
}

func NewStorage(ctx context.Context, connString string) (*Storage, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}
	return &Storage{db: pool}, nil
}

func (s *Storage) InsertBatch(ctx context.Context, telemetries []domain.Telemetry) error {
	if len(telemetries) == 0 {
		return nil
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, t := range telemetries {
		_, err := tx.Exec(ctx,
			"INSERT INTO location_history (time, asset_id, latitude, longitude) VALUES ($1, $2, $3, $4) ON CONFLICT (asset_id, time) DO NOTHING",
			t.Timestamp, t.AssetID, t.Latitude, t.Longitude)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Storage) GetRouteHistory(ctx context.Context, assetID string, start, end string) ([]domain.Telemetry, error) {
	query := `
		SELECT time, asset_id, latitude, longitude 
		FROM location_history 
		WHERE asset_id = $1 AND time >= $2 AND time <= $3 
		ORDER BY time ASC
	`
	rows, err := s.db.Query(ctx, query, assetID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var history []domain.Telemetry
	for rows.Next() {
		var t domain.Telemetry
		var timestamp time.Time
		if err := rows.Scan(&timestamp, &t.AssetID, &t.Latitude, &t.Longitude); err != nil {
			return nil, err
		}
		t.Timestamp = timestamp
		history = append(history, t)
	}

	return history, nil
}

func (s *Storage) GetDBMetrics(ctx context.Context) (map[string]interface{}, error) {
	metrics := make(map[string]interface{})
	
	var rowCount int64
	s.db.QueryRow(ctx, "SELECT count(*) FROM location_history").Scan(&rowCount)
	metrics["total_telemetry_rows"] = rowCount

	var assetCount int64
	s.db.QueryRow(ctx, "SELECT count(*) FROM assets").Scan(&assetCount)
	metrics["registered_assets"] = assetCount

	var activeConns int64
	s.db.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'").Scan(&activeConns)
	metrics["active_db_connections"] = activeConns

	var dbSize string
	s.db.QueryRow(ctx, "SELECT pg_size_pretty(pg_database_size('fleet'))").Scan(&dbSize)
	metrics["database_size"] = dbSize

	var totalBytes, compressedBytes int64
	err := s.db.QueryRow(ctx, "SELECT COALESCE(sum(total_bytes), 0), COALESCE(sum(compressed_total_bytes), 0) FROM hypertable_compression_stats('location_history')").Scan(&totalBytes, &compressedBytes)
	if err == nil && totalBytes > 0 && compressedBytes > 0 {
		metrics["compression_ratio_multiplier"] = fmt.Sprintf("%.2fx", float64(totalBytes)/float64(compressedBytes))
	} else {
		metrics["compression_ratio_multiplier"] = "1.00x (No compressed chunks yet)"
	}

	return metrics, nil
}

func (s *Storage) Close() {
	s.db.Close()
}
