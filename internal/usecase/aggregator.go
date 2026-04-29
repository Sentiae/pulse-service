package usecase

import (
	"context"
	"fmt"
	"sync"
	"time"

	opsv1 "github.com/sentiae/ops-service/gen/ops/v1"
	workv1 "github.com/sentiae/work-service/gen/proto/work/v1"
	"google.golang.org/grpc"
)

// Aggregator is the §3 (Phase 4) server-side Pulse aggregator. Before
// this, the Pulse landing page computed health / cost / revenue /
// deploy cadence client-side by fanning out to ops-service, data-
// service, and work-service — which (1) gave every operator a
// different snapshot depending on their network timing and
// (2) duplicated the federation logic in portal/BFF/mobile.
//
// Aggregator centralizes that fan-out via gRPC (CLAUDE.md §13). A
// single `GET /api/v1/pulse/summary` call returns a consistent
// snapshot the portal can render directly. Individual signals that
// fail to fetch surface as nil fields (not hard errors) so partial
// outages don't black out the landing page.
type Aggregator struct {
	opsArch     opsv1.OpsArchitectureServiceClient
	opsDeploy   opsv1.OpsDeploymentServiceClient
	opsIncident opsv1.OpsIncidentServiceClient
	workSpec    workv1.WorkSpecServiceClient
	cacheTTL    time.Duration

	mu          sync.RWMutex
	lastSnap    *Snapshot
	lastFetched time.Time
}

// AggregatorConfig captures cross-service gRPC connections. Each
// connection is optional — a nil one just means that signal gets
// skipped.
type AggregatorConfig struct {
	OpsConn  *grpc.ClientConn
	WorkConn *grpc.ClientConn
	// CacheTTL is how long a Snapshot is served from memory before a
	// fresh fan-out. Defaults to 30s — the Pulse landing refetches at
	// the same cadence so caching dampens the fan-out without making
	// the page feel stale.
	CacheTTL time.Duration
}

// NewAggregator wires the aggregator. Nil ClientConns are fine — the
// corresponding clients stay nil and their fan-out branch is skipped.
func NewAggregator(cfg AggregatorConfig) *Aggregator {
	ttl := cfg.CacheTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	a := &Aggregator{cacheTTL: ttl}
	if cfg.OpsConn != nil {
		a.opsArch = opsv1.NewOpsArchitectureServiceClient(cfg.OpsConn)
		a.opsDeploy = opsv1.NewOpsDeploymentServiceClient(cfg.OpsConn)
		a.opsIncident = opsv1.NewOpsIncidentServiceClient(cfg.OpsConn)
	}
	if cfg.WorkConn != nil {
		a.workSpec = workv1.NewWorkSpecServiceClient(cfg.WorkConn)
	}
	return a
}

// Snapshot is the aggregated Pulse view. Every field is a pointer so
// missing signals are distinguishable from zero values ("no incidents"
// vs "couldn't reach ops-service").
type Snapshot struct {
	GeneratedAt     time.Time      `json:"generated_at"`
	Health          *HealthSummary `json:"health,omitempty"`
	DeployCadence   *DeployCadence `json:"deploy_cadence,omitempty"`
	ActiveIncidents *int           `json:"active_incidents,omitempty"`
	OpenSpecs       *int           `json:"open_specs,omitempty"`
	SignalSources   []SignalSource `json:"signal_sources"`
}

// HealthSummary is the org-wide rollup of service health.
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

// SignalSource records which downstream contributed which signal +
// whether the fetch succeeded. Surfaces "degraded signals" banner.
type SignalSource struct {
	Service string `json:"service"`
	Signal  string `json:"signal"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// GetSnapshot returns the cached snapshot if fresh, otherwise fans out.
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

// fanOut hits every configured gRPC client in parallel.
func (a *Aggregator) fanOut(ctx context.Context, orgID string) *Snapshot {
	snap := &Snapshot{
		GeneratedAt:   time.Now().UTC(),
		SignalSources: []SignalSource{},
	}
	var mu sync.Mutex
	var wg sync.WaitGroup

	if a.opsArch != nil {
		wg.Add(1)
		go a.fetchHealth(ctx, orgID, snap, &mu, &wg)
	}
	if a.opsDeploy != nil {
		wg.Add(1)
		go a.fetchDeployCadence(ctx, orgID, snap, &mu, &wg)
	}
	if a.opsIncident != nil {
		wg.Add(1)
		go a.fetchIncidents(ctx, orgID, snap, &mu, &wg)
	}
	if a.workSpec != nil {
		wg.Add(1)
		go a.fetchOpenSpecs(ctx, orgID, snap, &mu, &wg)
	}

	wg.Wait()
	return snap
}

func (a *Aggregator) fetchHealth(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	resp, err := a.opsArch.GetLiveArchitectureMap(ctx, &opsv1.GetLiveArchitectureMapRequest{OrganizationId: orgID})
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "health", OK: false, Error: err.Error()})
		return
	}
	sum := &HealthSummary{}
	for _, n := range resp.GetNodes() {
		// LiveArchNode.Severity is ok|warn|critical (per ops proto).
		switch n.GetSeverity() {
		case "ok", "":
			sum.Healthy++
		case "warn":
			sum.Degraded++
		case "critical":
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
	resp, err := a.opsDeploy.GetDeploymentStats(ctx, &opsv1.GetDeploymentStatsRequest{OrganizationId: orgID})
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "deploy_cadence", OK: false, Error: err.Error()})
		return
	}
	out.DeployCadence = &DeployCadence{Last24h: int(resp.GetLast_24H()), SuccessRate: resp.GetSuccessRate()}
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "deploy_cadence", OK: true})
}

func (a *Aggregator) fetchIncidents(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	resp, err := a.opsIncident.GetIncidentStats(ctx, &opsv1.GetIncidentStatsRequest{OrganizationId: orgID})
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "incidents", OK: false, Error: err.Error()})
		return
	}
	n := int(resp.GetOpen())
	out.ActiveIncidents = &n
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "ops-service", Signal: "incidents", OK: true})
}

func (a *Aggregator) fetchOpenSpecs(ctx context.Context, orgID string, out *Snapshot, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	resp, err := a.workSpec.GetOrgSpecStats(ctx, &workv1.GetOrgSpecStatsRequest{OrganizationId: orgID})
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		out.SignalSources = append(out.SignalSources, SignalSource{Service: "work-service", Signal: "open_specs", OK: false, Error: err.Error()})
		return
	}
	// Open = anything not shipped/living. Sum all non-terminal status counts.
	open := 0
	for _, c := range resp.GetCounts() {
		st := c.GetStatus()
		if st == "shipped" || st == "living" {
			continue
		}
		open += int(c.GetCount())
	}
	out.OpenSpecs = &open
	out.SignalSources = append(out.SignalSources, SignalSource{Service: "work-service", Signal: "open_specs", OK: true})
}
