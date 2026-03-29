package management

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/store"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

type usageExportPayload struct {
	Version    int                      `json:"version"`
	ExportedAt time.Time                `json:"exported_at"`
	Usage      usage.StatisticsSnapshot `json:"usage"`
}

type usageImportPayload struct {
	Version int                      `json:"version"`
	Usage   usage.StatisticsSnapshot `json:"usage"`
}

type DBStatsResponse struct {
	TotalRequests  int64                  `json:"total_requests"`
	SuccessCount   int64                  `json:"success_count"`
	FailureCount   int64                  `json:"failure_count"`
	TotalTokens    int64                  `json:"total_tokens"`
	APIs           map[string]APIResponse `json:"apis"`
	RequestsByDay  map[string]int64       `json:"requests_by_day"`
	TokensByDay    map[string]int64       `json:"tokens_by_day"`
	RequestsByHour map[string]int64       `json:"requests_by_hour"`
	TokensByHour   map[string]int64       `json:"tokens_by_hour"`
}

type APIResponse struct {
	TotalRequests int64                    `json:"total_requests"`
	TotalTokens   int64                    `json:"total_tokens"`
	Models        map[string]ModelResponse `json:"models"`
}

type ModelResponse struct {
	TotalRequests int64 `json:"total_requests"`
	TotalTokens   int64 `json:"total_tokens"`
}

func (h *Handler) GetUsageStatistics(c *gin.Context) {
	ctx := c.Request.Context()

	if h.usageStore != nil {
		h.getStatsFromDB(ctx, c)
		return
	}

	h.getStatsFromMemory(c)
}

func (h *Handler) getStatsFromDB(ctx context.Context, c *gin.Context) {
	totalStats, err := h.usageStore.GetTotalStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	apiStats, err := h.usageStore.GetAPIStats(ctx)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	days := 30
	if d := c.Query("days"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed > 0 && parsed <= 365 {
			days = parsed
		}
	}

	reqByDay, tokByDay, err := h.usageStore.GetDailyStats(ctx, days)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	reqByHour, tokByHour, err := h.usageStore.GetHourlyStats(ctx, days)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	apis := make(map[string]APIResponse)
	for apiKey, stats := range apiStats {
		models := make(map[string]ModelResponse)
		for model, modelStats := range stats.Models {
			models[model] = ModelResponse{
				TotalRequests: modelStats.TotalRequests,
				TotalTokens:   modelStats.TotalTokens,
			}
		}
		apis[apiKey] = APIResponse{
			TotalRequests: stats.TotalRequests,
			TotalTokens:   stats.TotalTokens,
			Models:        models,
		}
	}

	c.JSON(http.StatusOK, DBStatsResponse{
		TotalRequests:  totalStats.TotalRequests,
		SuccessCount:   totalStats.SuccessCount,
		FailureCount:   totalStats.FailureCount,
		TotalTokens:    totalStats.TotalTokens,
		APIs:           apis,
		RequestsByDay:  reqByDay,
		TokensByDay:    tokByDay,
		RequestsByHour: reqByHour,
		TokensByHour:   tokByHour,
	})
}

func (h *Handler) getStatsFromMemory(c *gin.Context) {
	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.JSON(http.StatusOK, gin.H{
		"usage":           snapshot,
		"failed_requests": snapshot.FailureCount,
	})
}

func (h *Handler) ExportUsageStatistics(c *gin.Context) {
	if h.usageStore != nil {
		ctx := c.Request.Context()
		totalStats, err := h.usageStore.GetTotalStats(ctx)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		apiStats, _ := h.usageStore.GetAPIStats(ctx)
		reqByDay, tokByDay, _ := h.usageStore.GetDailyStats(ctx, 365)
		reqByHour, tokByHour, _ := h.usageStore.GetHourlyStats(ctx, 365)

		c.JSON(http.StatusOK, DBStatsResponse{
			TotalRequests:  totalStats.TotalRequests,
			SuccessCount:   totalStats.SuccessCount,
			FailureCount:   totalStats.FailureCount,
			TotalTokens:    totalStats.TotalTokens,
			APIs:           convertAPIStats(apiStats),
			RequestsByDay:  reqByDay,
			TokensByDay:    tokByDay,
			RequestsByHour: reqByHour,
			TokensByHour:   tokByHour,
		})
		return
	}

	var snapshot usage.StatisticsSnapshot
	if h != nil && h.usageStats != nil {
		snapshot = h.usageStats.Snapshot()
	}
	c.JSON(http.StatusOK, usageExportPayload{
		Version:    1,
		ExportedAt: time.Now().UTC(),
		Usage:      snapshot,
	})
}

func (h *Handler) ImportUsageStatistics(c *gin.Context) {
	if h.usageStore != nil {
		c.JSON(http.StatusOK, gin.H{
			"message": "DB-based storage does not support import. Use DB directly.",
		})
		return
	}

	if h == nil || h.usageStats == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "usage statistics unavailable"})
		return
	}

	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read request body"})
		return
	}

	var payload usageImportPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if payload.Version != 0 && payload.Version != 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported version"})
		return
	}

	result := h.usageStats.MergeSnapshot(payload.Usage)
	snapshot := h.usageStats.Snapshot()
	c.JSON(http.StatusOK, gin.H{
		"added":           result.Added,
		"skipped":         result.Skipped,
		"total_requests":  snapshot.TotalRequests,
		"failed_requests": snapshot.FailureCount,
	})
}

func (h *Handler) QueryUsage(c *gin.Context) {
	if h.usageStore == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "DB store not available"})
		return
	}

	ctx := c.Request.Context()
	apiKey := c.Query("api_key")
	model := c.Query("model")
	limit := 100
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}

	records, err := h.usageStore.QueryRequests(ctx, apiKey, model, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"records": records, "count": len(records)})
}

func convertAPIStats(stats map[string]*store.APIStatsResult) map[string]APIResponse {
	result := make(map[string]APIResponse)
	for apiKey, s := range stats {
		models := make(map[string]ModelResponse)
		for model, modelStats := range s.Models {
			models[model] = ModelResponse{
				TotalRequests: modelStats.TotalRequests,
				TotalTokens:   modelStats.TotalTokens,
			}
		}
		result[apiKey] = APIResponse{
			TotalRequests: s.TotalRequests,
			TotalTokens:   s.TotalTokens,
			Models:        models,
		}
	}
	return result
}
