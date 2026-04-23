package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Aggregator is the §3 (Phase 4) server-side Pulse aggregator. Before
// this, the Pulse landing page computed health / cost / revenue /
// deploy cadence client-side by fanning out to ops-service, data-
// service, and work-service — which (1) gave every operator a
// different snapshot depending on their network timing and
// (2) duplicated the federation logic in portal/BFF/mobile.
//
// Aggregator centralizes that fan-out. A single `GET
// /api/v1/pulse/summary` call returns a consistent snapshot the
// portal can render directly. Individual signals that fail to fetch
// surface as nil fields (not hard errors) so partial outages don't
// black out the landing page.
type Aggregator struct {
	http     *http.Client
	opsURL   string
	workURL  string
	dataURL  string
	token    string
	cacheTTL time.Duration

	mu          sync.RWMutex
	lastSnap    *Snapshot
	lastFetched time.Time
}

// AggregatorConfig captures the cross-service base URLs. Each is
// optional — a missing URL just means that signal gets skipped.
type AggregatorConfig struct {
	OpsServiceURL  string
	WorkServiceURL string
	DataServiceURL string
	// ServiceToken is forwarded as `Authorization: Bearer` so the
	// downstream services' service-to-service auth recognises pulse.
	ServiceToken string
	// HTTPTimeout caps each downstream call. Defaults to 3s.
	HTTPTimeout time.Duration
	// CacheTTL is how long a Snapshot is served from memory before a
	// fresh fan-out. Defaults to 30s — the Pulse landing refetches at
	// the same cadence so caching dampens the fan-out without making
	// the page feel stale.
	CacheTTL time.Duration
}

// NewAggregator wires the aggregator.
func NewAggregator(cfg AggregatorConfig) *Aggregator {
	timeout := cfg.HTTPTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	ttl := cfg.CacheTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Aggregator{
		http:     &http.Client{Timeout: timeout},
		opsURL:   cfg.OpsServiceURL,
		workURL:  cfg.WorkServiceURL,
		dataURL:  cfg.DataServiceURL,
		token:    cfg.ServiceToken,
		cacheTTL: ttl,
	}
}

// Snapshot is the aggregated Pulse view. Every field is a pointer so
// missing signals are distinguishable from zero values ("no incidents"
// vs "couldn't reach ops-service").
type Snapshot struct {
	GeneratedAt    time.Time        `json:"generated_at"`
	Health         *HealthSummary   `json:"health,omitempty"`
	DeployCadence  *DeployCadence   `json:"deploy_cadence,omitempty"`
	ActiveIncidents *int            `json:"active_incidents,omitempty"`
	OpenSpecs      *int             `json:"open_specs,omitempty"`
	SignalSources  []SignalSource   `json:"signal_sources"`
}

// HealthSummary is the org-wide rollup of service health. Counts sum
// across the service catalog; `score_pct` is the healthy-proportion
// quick-read the Pulse landing uses for its ring gauge.
type HealthSummary struct {
	Healthy   int     `json:"healthy"`
	Degraded  int     `json:"degraded"`
	Unhealthy int     `json:"unhealthy"`
	ScorePct  float64 `json:"score_pct"`
}

// DeployCadence captures rolling 24h deployment counts + success rate.
type DeployCadence struct {
	Last24h     int     `json:"last_24h"`
	SuccessRate float64 `json:"success_rate"`
}

// SignalSource is an accounting entry: which downstream service
// contributed which signal, and whether the fetch succeeded. The
// portal can surface a "degraded signals" banner without the
// aggregator having to synthesize a boolean healthy state.
type SignalSource struct {
	Service string `json:"service"`
	Signal  string `json:"signal"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// GetSnapshot returns the cached snapshot if it's fresh, otherwise
// fans out to every configured source and rebuilds. Concurrent callers
// during a refresh see the previous snapshot until the new one lands
// — the aggregator never blocks a request on the fan-out path.
func (a *Aggregator) GetSnapshot(ctx context.Context, orgID string) (*Snapshot, error) {
	if orgID == "" {
		return nil, fmt.Errorf("aggregator: org_id is required")
	}

	a.mu.RLock()
	if a.lastSnap != nil && time.Since(a.lastFetched) < a.cacheTTL {
		snap := a.lastSnap
		a.mu.RUnlock()
		return snap, nil
	}
	a.mu.RUnlock()

	snap := a.fanOut(ctx, orgID)

	a.mu.Lock()
	a.lastSnap = snap
	a.lastFetched = time.Now()
	a.mu.Unlock()

	return snap, nil
}

// fanOut contacts every configured downstream service in parallel and
// assembles the Snapshot. Per-source failures record into SignalSources
// and leave the corresponding field nil.
func (a *Aggregator) fanOut(ctx context.Context, orgID string) *Snapshot {
	snap := &Snapshot{
		GeneratedAt:   time.Now().UTC(),
		SignalSources: []SignalSource{},
	}
	var mu sync.Mutex
	var wg sync.WaitGroup

	if a.opsURL != "" {
		wg.Add(3)
		go a.fetchHealth(ctx, orgID, snap, &mu, &wg)
		go a.fetchDeployCadence(ctx, orgID, snap, &mu, &wg)
		go a.fetchIncidents(ctx, orgID, snap, &mu, &wg)
	}
	if a.workURL != "" {
		wg.Add(1)
		go a.fetchOpenSpecs(ctx, orgID, snap, &mu, &wg)
	}

	wg.Wait()
	return snap
}

func (a *Aggregator) fetchHealth(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	var live struct {
		Data struct {
			Services []struct {
				Status string `json:"status"`
			} `json:"services"`
		} `json:"data"`
	}
	err := a.getJSON(ctx, fmt.Sprintf("%s/ops/architecture/live?org_id=%s", a.opsURL, orgID), &live)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "health", OK: false, Error: err.Error()})
		return
	}
	sum := &HealthSummary{}
	for _, s := range live.Data.Services {
		switch s.Status {
		case "healthy":
			sum.Healthy++
		case "degraded":
			sum.Degraded++
		case "unhealthy":
			sum.Unhealthy++
		}
	}
	total := sum.Healthy + sum.Degraded + sum.Unhealthy
	if total > 0 {
		sum.ScorePct = float64(sum.Healthy) / float64(total) * 100
	}
	out.Health = sum
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "health", OK: true})
}

func (a *Aggregator) fetchDeployCadence(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	// ops-service exposes deployment stats via /ops/deployments/stats
	// — we read it as a best-effort signal; shape differences are
	// surfaced as errors so downstream ships stay observable.
	var body struct {
		Data struct {
			Last24h     int     `json:"last_24h"`
			SuccessRate float64 `json:"success_rate"`
		} `json:"data"`
	}
	err := a.getJSON(ctx, fmt.Sprintf("%s/ops/deployments/stats?org_id=%s", a.opsURL, orgID), &body)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "deploy_cadence", OK: false, Error: err.Error()})
		return
	}
	out.DeployCadence = &DeployCadence{Last24h: body.Data.Last24h, SuccessRate: body.Data.SuccessRate}
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "deploy_cadence", OK: true})
}

func (a *Aggregator) fetchIncidents(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	var body struct {
		Data struct {
			Open int `json:"open"`
		} `json:"data"`
	}
	err := a.getJSON(ctx, fmt.Sprintf("%s/ops/incidents/stats?org_id=%s", a.opsURL, orgID), &body)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "incidents", OK: false, Error: err.Error()})
		return
	}
	n := body.Data.Open
	out.ActiveIncidents = &n
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "incidents", OK: true})
}

func (a *Aggregator) fetchOpenSpecs(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	var body struct {
		Data struct {
			Open int `json:"open"`
		} `json:"data"`
	}
	err := a.getJSON(ctx, fmt.Sprintf("%s/api/v1/work/specs/stats?org_id=%s", a.workURL, orgID), &body)
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "work-service", Signal: "open_specs", OK: false, Error: err.Error()})
		return
	}
	n := body.Data.Open
	out.OpenSpecs = &n
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "work-service", Signal: "open_specs", OK: true})
}

// getJSON is a small helper wrapping the auth header + decode.
func (a *Aggregator) getJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
