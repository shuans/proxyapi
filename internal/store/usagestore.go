package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
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

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return fmt.Errorf("usage store: open database: %w", err)
	}
	db.SetMaxOpenConns(1)
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
	CREATE TABLE IF NOT EXISTS requests (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp TEXT NOT NULL DEFAULT (datetime('now')),
		api_key TEXT NOT NULL,
		model TEXT NOT NULL,
		source TEXT,
		auth_index TEXT,
		input_tokens INTEGER NOT NULL DEFAULT 0,
		output_tokens INTEGER NOT NULL DEFAULT 0,
		reasoning_tokens INTEGER NOT NULL DEFAULT 0,
		cached_tokens INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		latency_ms INTEGER NOT NULL DEFAULT 0,
		failed INTEGER NOT NULL DEFAULT 0,
		provider TEXT,
		request_id TEXT
	);

	CREATE INDEX IF NOT EXISTS idx_requests_timestamp ON requests(timestamp);
	CREATE INDEX IF NOT EXISTS idx_requests_api_key ON requests(api_key);
	CREATE INDEX IF NOT EXISTS idx_requests_model ON requests(model);

	CREATE TABLE IF NOT EXISTS daily_stats (
		date_key TEXT PRIMARY KEY,
		total_requests INTEGER NOT NULL DEFAULT 0,
		success_requests INTEGER NOT NULL DEFAULT 0,
		failed_requests INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS hourly_stats (
		hour INTEGER NOT NULL,
		date_key TEXT NOT NULL,
		total_requests INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(hour, date_key)
	);

	CREATE TABLE IF NOT EXISTS api_stats (
		api_key TEXT PRIMARY KEY,
		total_requests INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS model_stats (
		api_key TEXT NOT NULL,
		model TEXT NOT NULL,
		total_requests INTEGER NOT NULL DEFAULT 0,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(api_key, model)
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

	now := time.Now()
	timestamp := now.Format("2006-01-02 15:04:05")
	dayKey := now.Format("2006-01-02")
	hourKey := now.Hour()

	_, err := s.db.ExecContext(ctx,
		"INSERT INTO requests (timestamp, api_key, model, source, auth_index, input_tokens, output_tokens, reasoning_tokens, cached_tokens, total_tokens, latency_ms, failed, provider, request_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		timestamp, record.APIKey, record.Model, record.Source, record.AuthIndex,
		record.InputTokens, record.OutputTokens, record.ReasoningTokens, record.CachedTokens, record.TotalTokens,
		record.LatencyMs, boolToInt(record.Failed), record.Provider, record.RequestID)

	if err != nil {
		return fmt.Errorf("usage store: insert request: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO daily_stats (date_key, total_requests, success_requests, failed_requests, total_tokens) VALUES (?, 1, ?, ?, ?) ON CONFLICT(date_key) DO UPDATE SET total_requests = total_requests + 1, success_requests = success_requests + ?, failed_requests = failed_requests + ?, total_tokens = total_tokens + ?",
		dayKey, boolToInt(!record.Failed), boolToInt(record.Failed), record.TotalTokens,
		boolToInt(!record.Failed), boolToInt(record.Failed), record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update daily stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO hourly_stats (hour, date_key, total_requests, total_tokens) VALUES (?, ?, 1, ?) ON CONFLICT(hour, date_key) DO UPDATE SET total_requests = total_requests + 1, total_tokens = total_tokens + ?",
		hourKey, dayKey, record.TotalTokens, record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update hourly stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO api_stats (api_key, total_requests, total_tokens) VALUES (?, 1, ?) ON CONFLICT(api_key) DO UPDATE SET total_requests = total_requests + 1, total_tokens = total_tokens + ?",
		record.APIKey, record.TotalTokens, record.TotalTokens)
	if err != nil {
		log.Warnf("usage store: update api stats failed: %v", err)
	}

	_, err = s.db.ExecContext(ctx,
		"INSERT INTO model_stats (api_key, model, total_requests, total_tokens) VALUES (?, ?, 1, ?) ON CONFLICT(api_key, model) DO UPDATE SET total_requests = total_requests + 1, total_tokens = total_tokens + ?",
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

	startDate := time.Now().AddDate(0, 0, -days).Format("2006-01-02")

	rows, err := s.db.QueryContext(ctx,
		"SELECT date_key, total_requests, total_tokens FROM daily_stats WHERE date_key >= ? ORDER BY date_key DESC LIMIT ?",
		startDate, days)
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

	startDate := time.Now().AddDate(0, 0, -days).Format("2006-01-02")

	rows, err := s.db.QueryContext(ctx,
		"SELECT hour, SUM(total_requests), SUM(total_tokens) FROM hourly_stats WHERE date_key >= ? GROUP BY hour ORDER BY hour",
		startDate)
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
		var failed int
		if err := rows.Scan(&r.APIKey, &r.Model, &r.Source, &r.AuthIndex,
			&r.InputTokens, &r.OutputTokens, &r.ReasoningTokens, &r.CachedTokens, &r.TotalTokens,
			&r.LatencyMs, &failed, &r.Provider, &r.RequestID); err != nil {
			continue
		}
		r.Failed = failed == 1
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
