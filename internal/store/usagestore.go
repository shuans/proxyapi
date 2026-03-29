package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
	log "github.com/sirupsen/logrus"
)

type UsageStoreConfig struct {
	Dir string
}

type UsageStore struct {
	db  *sql.DB
	dir string
	mu  sync.Mutex
}

func NewUsageStore(cfg UsageStoreConfig) (*UsageStore, error) {
	dir := cfg.Dir
	if dir == "" {
		if cwd, err := os.Getwd(); err == nil {
			dir = filepath.Join(cwd, "data")
		} else {
			dir = os.TempDir()
		}
	}

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("usage store: create directory: %w", err)
	}

	store := &UsageStore{dir: dir}
	if err := store.init(); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *UsageStore) init() error {
	dbPath := filepath.Join(s.dir, "usage_stats.db")

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("usage store: open database: %w", err)
	}
	s.db = db

	if err := s.ensureSchema(); err != nil {
		_ = db.Close()
		return err
	}

	return nil
}

func (s *UsageStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *UsageStore) ensureSchema() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage store: not initialized")
	}

	schema := `
	CREATE SEQUENCE IF NOT EXISTS requests_id_seq;
	CREATE TABLE IF NOT EXISTS requests (
		id BIGINT DEFAULT nextval('requests_id_seq') PRIMARY KEY,
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		api_key VARCHAR NOT NULL,
		model VARCHAR NOT NULL,
		source VARCHAR,
		auth_index VARCHAR,
		input_tokens BIGINT NOT NULL DEFAULT 0,
		output_tokens BIGINT NOT NULL DEFAULT 0,
		reasoning_tokens BIGINT NOT NULL DEFAULT 0,
		cached_tokens BIGINT NOT NULL DEFAULT 0,
		total_tokens BIGINT NOT NULL DEFAULT 0,
		latency_ms BIGINT NOT NULL DEFAULT 0,
		failed BOOLEAN NOT NULL DEFAULT false,
		provider VARCHAR,
		request_id VARCHAR
	);

	CREATE SEQUENCE IF NOT EXISTS daily_stats_id_seq;
	CREATE TABLE IF NOT EXISTS daily_stats (
		id BIGINT DEFAULT nextval('daily_stats_id_seq') PRIMARY KEY,
		date_key DATE UNIQUE,
		total_requests BIGINT NOT NULL DEFAULT 0,
		success_requests BIGINT NOT NULL DEFAULT 0,
		failed_requests BIGINT NOT NULL DEFAULT 0,
		total_tokens BIGINT NOT NULL DEFAULT 0
	);

	CREATE SEQUENCE IF NOT EXISTS hourly_stats_id_seq;
	CREATE TABLE IF NOT EXISTS hourly_stats (
		id BIGINT DEFAULT nextval('hourly_stats_id_seq') PRIMARY KEY,
		hour INTEGER NOT NULL,
		date_key DATE NOT NULL,
		total_requests BIGINT NOT NULL DEFAULT 0,
		total_tokens BIGINT NOT NULL DEFAULT 0,
		UNIQUE(hour, date_key)
	);

	CREATE SEQUENCE IF NOT EXISTS api_stats_id_seq;
	CREATE TABLE IF NOT EXISTS api_stats (
		id BIGINT DEFAULT nextval('api_stats_id_seq') PRIMARY KEY,
		api_key VARCHAR UNIQUE,
		total_requests BIGINT NOT NULL DEFAULT 0,
		total_tokens BIGINT NOT NULL DEFAULT 0
	);

	CREATE SEQUENCE IF NOT EXISTS model_stats_id_seq;
	CREATE TABLE IF NOT EXISTS model_stats (
		id BIGINT DEFAULT nextval('model_stats_id_seq') PRIMARY KEY,
		api_key VARCHAR NOT NULL,
		model VARCHAR NOT NULL,
		total_requests BIGINT NOT NULL DEFAULT 0,
		total_tokens BIGINT NOT NULL DEFAULT 0,
		UNIQUE(api_key, model)
	);
	`

	_, err := s.db.Exec(schema)
	return err
}

type UsageRecord struct {
	APIKey          string
	Model           string
	Source          string
	AuthIndex       string
	InputTokens     int64
	OutputTokens    int64
	ReasoningTokens int64
	CachedTokens    int64
	TotalTokens     int64
	LatencyMs       int64
	Failed          bool
	Provider        string
	RequestID       string
}

func (s *UsageStore) InsertRecord(ctx context.Context, record UsageRecord) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("usage store: not initialized")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO requests (api_key, model, source, auth_index, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, latency_ms, failed, provider, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		record.APIKey, record.Model, record.Source, record.AuthIndex,
		record.InputTokens, record.OutputTokens, record.ReasoningTokens, record.CachedTokens, record.TotalTokens,
		record.LatencyMs, record.Failed, record.Provider, record.RequestID)

	if err != nil {
		return fmt.Errorf("usage store: insert request: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO daily_stats (date_key, total_requests, success_requests, failed_requests, total_tokens) VALUES (CURRENT_DATE, 1, ?, ?, ?) ON CONFLICT(date_key) DO UPDATE SET total_requests = daily_stats.total_requests + 1, success_requests = daily_stats.success_requests + ?, failed_requests = daily_stats.failed_requests + ?, total_tokens = daily_stats.total_tokens + ?",
		boolToInt(!record.Failed), boolToInt(record.Failed), record.TotalTokens,
		boolToInt(!record.Failed), boolToInt(record.Failed), record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update daily stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO hourly_stats (hour, date_key, total_requests, total_tokens) VALUES (EXTRACT(HOUR FROM CURRENT_TIMESTAMP)::INTEGER, CURRENT_DATE, 1, ?) ON CONFLICT(hour, date_key) DO UPDATE SET total_requests = hourly_stats.total_requests + 1, total_tokens = hourly_stats.total_tokens + ?",
		record.TotalTokens, record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update hourly stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO api_stats (api_key, total_requests, total_tokens) VALUES (?, 1, ?) ON CONFLICT(api_key) DO UPDATE SET total_requests = api_stats.total_requests + 1, total_tokens = api_stats.total_tokens + ?",
		record.APIKey, record.TotalTokens, record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update api stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO model_stats (api_key, model, total_requests, total_tokens) VALUES (?, ?, 1, ?) ON CONFLICT(api_key, model) DO UPDATE SET total_requests = model_stats.total_requests + 1, total_tokens = model_stats.total_tokens + ?",
		record.APIKey, record.Model, record.TotalTokens, record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update model stats failed: %v", err)
	}

	return nil
}

type TotalStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailureCount  int64
	TotalTokens   int64
}

func (s *UsageStore) GetTotalStats(ctx context.Context) (*TotalStats, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("usage store: not initialized")
	}

	var totalRequests, successCount, failureCount, totalTokens int64
	err := s.db.QueryRowContext(ctx,
		"SELECT COALESCE(SUM(total_requests), 0), COALESCE(SUM(success_requests), 0), COALESCE(SUM(failed_requests), 0), COALESCE(SUM(total_tokens), 0) FROM daily_stats",
	).Scan(&totalRequests, &successCount, &failureCount, &totalTokens)

	return &TotalStats{
		TotalRequests: totalRequests,
		SuccessCount:  successCount,
		FailureCount:  failureCount,
		TotalTokens:   totalTokens,
	}, err
}

type APIStatsResult struct {
	TotalRequests int64
	TotalTokens   int64
	Models        map[string]ModelStatsResult
}

type ModelStatsResult struct {
	TotalRequests int64
	TotalTokens   int64
}

func (s *UsageStore) GetAPIStats(ctx context.Context) (map[string]*APIStatsResult, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("usage store: not initialized")
	}

	rows, err := s.db.QueryContext(ctx, "SELECT api_key, total_requests, total_tokens FROM api_stats ORDER BY total_requests DESC")
	if err != nil {
		return nil, fmt.Errorf("usage store: query api stats: %w", err)
	}
	defer rows.Close()

	result := make(map[string]*APIStatsResult)
	for rows.Next() {
		var apiKey string
		var totalRequests, totalTokens int64
		if err := rows.Scan(&apiKey, &totalRequests, &totalTokens); err != nil {
			continue
		}
		result[apiKey] = &APIStatsResult{
			TotalRequests: totalRequests,
			TotalTokens:   totalTokens,
			Models:        make(map[string]ModelStatsResult),
		}
	}

	for apiKey := range result {
		rows, err := s.db.QueryContext(ctx,
			"SELECT model, total_requests, total_tokens FROM model_stats WHERE api_key = ? ORDER BY total_requests DESC", apiKey)
		if err != nil {
			continue
		}

		for rows.Next() {
			var model string
			var totalRequests, totalTokens int64
			if err := rows.Scan(&model, &totalRequests, &totalTokens); err != nil {
				rows.Close()
				continue
			}
			result[apiKey].Models[model] = ModelStatsResult{
				TotalRequests: totalRequests,
				TotalTokens:   totalTokens,
			}
		}
		rows.Close()
	}

	return result, nil
}

func (s *UsageStore) GetDailyStats(ctx context.Context, days int) (map[string]int64, map[string]int64, error) {
	if s == nil || s.db == nil {
		return nil, nil, fmt.Errorf("usage store: not initialized")
	}

	rows, err := s.db.QueryContext(ctx,
		"SELECT date_key, total_requests, total_tokens FROM daily_stats WHERE date_key >= CURRENT_DATE - INTERVAL '1 day' * ? ORDER BY date_key DESC LIMIT ?",
		days, days)
	if err != nil {
		return nil, nil, fmt.Errorf("usage store: query daily stats: %w", err)
	}
	defer rows.Close()

	requests := make(map[string]int64)
	tokens := make(map[string]int64)
	for rows.Next() {
		var dateKey string
		var reqCount, tokCount int64
		if err := rows.Scan(&dateKey, &reqCount, &tokCount); err != nil {
			continue
		}
		requests[dateKey] = reqCount
		tokens[dateKey] = tokCount
	}

	return requests, tokens, nil
}

func (s *UsageStore) GetHourlyStats(ctx context.Context, days int) (map[string]int64, map[string]int64, error) {
	if s == nil || s.db == nil {
		return nil, nil, fmt.Errorf("usage store: not initialized")
	}

	rows, err := s.db.QueryContext(ctx,
		"SELECT hour, SUM(total_requests), SUM(total_tokens) FROM hourly_stats WHERE date_key >= CURRENT_DATE - INTERVAL '1 day' * ? GROUP BY hour ORDER BY hour",
		days)
	if err != nil {
		return nil, nil, fmt.Errorf("usage store: query hourly stats: %w", err)
	}
	defer rows.Close()

	requests := make(map[string]int64)
	tokens := make(map[string]int64)
	for rows.Next() {
		var hour int
		var reqCount, tokCount int64
		if err := rows.Scan(&hour, &reqCount, &tokCount); err != nil {
			continue
		}
		requests[fmt.Sprintf("%02d", hour)] = reqCount
		tokens[fmt.Sprintf("%02d", hour)] = tokCount
	}

	return requests, tokens, nil
}

func (s *UsageStore) QueryRequests(ctx context.Context, apiKey, model string, limit int) ([]UsageRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("usage store: not initialized")
	}

	sql := "SELECT api_key, model, source, auth_index, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, latency_ms, failed, provider, request_id FROM requests WHERE 1=1"
	args := []any{}

	if apiKey != "" {
		sql += " AND api_key = ?"
		args = append(args, apiKey)
	}
	if model != "" {
		sql += " AND model = ?"
		args = append(args, model)
	}

	sql += " ORDER BY timestamp DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("usage store: query requests: %w", err)
	}
	defer rows.Close()

	var results []UsageRecord
	for rows.Next() {
		var r UsageRecord
		var failed bool
		if err := rows.Scan(&r.APIKey, &r.Model, &r.Source, &r.AuthIndex,
			&r.InputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.CachedTokens, &r.TotalTokens,
			&r.LatencyMs, &failed, &r.Provider, &r.RequestID); err != nil {
			continue
		}
		r.Failed = failed
		results = append(results, r)
	}

	return results, nil
}

func (s *UsageStore) GetRecentRequests(ctx context.Context, limit int) ([]UsageRecord, error) {
	return s.QueryRequests(ctx, "", "", limit)
}

func boolToInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
