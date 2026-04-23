package usecase

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mockOps stands in for ops-service for the three signals the
// aggregator pulls. Each endpoint records a hit so we can assert on
// caching + fan-out behavior.
type mockOps struct {
	health    int64
	cadence   int64
	incidents int64
	srv       *httptest.Server
}

func newMockOps(t *testing.T) *mockOps {
	t.Helper()
	m := &mockOps{}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/architecture/live"):
			atomic.AddInt64(&m.health, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"services": []map[string]any{
						{"status": "healthy"},
						{"status": "healthy"},
						{"status": "degraded"},
					},
				},
			})
		case strings.Contains(r.URL.Path, "/deployments/stats"):
			atomic.AddInt64(&m.cadence, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"last_24h":     7,
					"success_rate": 0.85,
				},
			})
		case strings.Contains(r.URL.Path, "/incidents/stats"):
			atomic.AddInt64(&m.incidents, 1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"open": 2},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	return m
}

func (m *mockOps) close() { m.srv.Close() }

func TestAggregator_FanOutComposesSnapshot(t *testing.T) {
	ops := newMockOps(t)
	defer ops.close()

	agg := NewAggregator(AggregatorConfig{
		OpsServiceURL: ops.srv.URL,
		CacheTTL:      1 * time.Millisecond, // so repeated calls re-fan-out
		HTTPTimeout:   2 * time.Second,
	})

	snap, err := agg.GetSnapshot(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Health == nil || snap.Health.Healthy != 2 || snap.Health.Degraded != 1 {
		t.Fatalf("unexpected health rollup: %+v", snap.Health)
	}
	if snap.Health.ScorePct < 66 || snap.Health.ScorePct > 67 {
		t.Fatalf("expected ~66.67%% healthy, got %f", snap.Health.ScorePct)
	}
	if snap.DeployCadence == nil || snap.DeployCadence.Last24h != 7 {
		t.Fatalf("cadence missing or wrong: %+v", snap.DeployCadence)
	}
	if snap.ActiveIncidents == nil || *snap.ActiveIncidents != 2 {
		t.Fatalf("incidents missing or wrong: %+v", snap.ActiveIncidents)
	}
	if len(snap.SignalSources) != 3 {
		t.Fatalf("expected 3 signal sources, got %d", len(snap.SignalSources))
	}
	for _, s := range snap.SignalSources {
		if !s.OK {
			t.Fatalf("all sources should be OK here, got %+v", s)
		}
	}
}

func TestAggregator_CacheHitSkipsFanOut(t *testing.T) {
	ops := newMockOps(t)
	defer ops.close()

	agg := NewAggregator(AggregatorConfig{
		OpsServiceURL: ops.srv.URL,
		CacheTTL:      1 * time.Hour, // cache persists across the test
		HTTPTimeout:   2 * time.Second,
	})
	// First call fans out.
	_, _ = agg.GetSnapshot(context.Background(), "org-1")
	healthFirst := atomic.LoadInt64(&ops.health)

	// Second call should hit the cache.
	_, _ = agg.GetSnapshot(context.Background(), "org-1")
	if got := atomic.LoadInt64(&ops.health); got != healthFirst {
		t.Fatalf("cache miss on second call: health calls jumped from %d to %d", healthFirst, got)
	}
}

func TestAggregator_FailedSignalRecordedButDoesntBreakOthers(t *testing.T) {
	// ops-service returns 500 for incidents but healthy responses
	// for the other two. The snapshot must still surface the
	// successful signals + record the failure in SignalSources.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/incidents/stats") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/architecture/live") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"services": []map[string]any{{"status": "healthy"}},
				},
			})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{"last_24h": 1, "success_rate": 1.0},
			})
		}
	}))
	defer srv.Close()

	agg := NewAggregator(AggregatorConfig{OpsServiceURL: srv.URL, CacheTTL: 1 * time.Millisecond})
	snap, err := agg.GetSnapshot(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Health == nil {
		t.Fatal("health signal should have succeeded")
	}
	if snap.DeployCadence == nil {
		t.Fatal("cadence signal should have succeeded")
	}
	if snap.ActiveIncidents != nil {
		t.Fatal("incidents signal failed; ActiveIncidents must be nil")
	}
	// At least one source must record the failure.
	failed := 0
	for _, s := range snap.SignalSources {
		if !s.OK && s.Signal == "incidents" {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("expected 1 failed incidents source entry, got %d", failed)
	}
}

func TestAggregator_MissingOrgIDErrors(t *testing.T) {
	agg := NewAggregator(AggregatorConfig{OpsServiceURL: "http://x"})
	_, err := agg.GetSnapshot(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty org_id")
	}
}
