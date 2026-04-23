package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/sentiae/pulse-service/internal/domain"
	"github.com/sentiae/pulse-service/internal/repository/postgres"
	"github.com/sentiae/pulse-service/internal/usecase"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// Server wires the REST + WebSocket surface of pulse-service.
type Server struct {
	router        chi.Router
	tracker       *usecase.FlowTracker
	recorder      *usecase.AuditRecorder
	aggregator    *usecase.Aggregator
	alertTracker  *usecase.AlertTracker
	deployTracker *usecase.DeployTracker
	wsUp          websocket.Upgrader
}

func NewServer(tracker *usecase.FlowTracker, recorder *usecase.AuditRecorder) *Server {
	s := &Server{
		tracker:  tracker,
		recorder: recorder,
		wsUp: websocket.Upgrader{
			// Pulse is behind the BFF in production; accept all origins in
			// dev and let the BFF enforce origin checks.
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
	s.setupRoutes()
	return s
}

// SetActivityTrackers wires the §3.1/§3.2 alert + deploy activity
// trackers. Kept as a setter so DI can defer handler construction and
// still pass the trackers through once they're built.
func (s *Server) SetActivityTrackers(alerts *usecase.AlertTracker, deploys *usecase.DeployTracker) {
	s.alertTracker = alerts
	s.deployTracker = deploys
}

// SetAggregator wires the §3 Pulse aggregator. Kept as a setter so DI
// can decide whether to enable it based on cross-service URL config —
// without configured downstream URLs the aggregator has nothing to
// pull so disabling the route is the honest answer.
func (s *Server) SetAggregator(a *usecase.Aggregator) {
	s.aggregator = a
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.router.ServeHTTP(w, r) }

func (s *Server) setupRoutes() {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "service": "pulse"})
	})
	r.Get("/ready", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	r.Route("/api/v1/pulse", func(r chi.Router) {
		r.Get("/flows", s.listFlows)
		r.Get("/flows/active", s.listActiveFlows)
		r.Get("/flows/stats", s.flowStats)
		r.Get("/flows/stream", s.streamFlows)
		r.Get("/flows/{flowID}", s.getFlow)
		r.Post("/flows/{flowID}/replay", s.replayFlow)
		// §3 Pulse aggregator — federated health/deploy/incident snapshot.
		// Skipped when aggregator is nil (no downstream URLs configured).
		r.Get("/summary", s.pulseSummary)

		// §3.1/§3.2 activity streams — live alert + deploy feeds for the
		// Pulse landing.
		r.Get("/activity/alerts", s.listAlertActivity)
		r.Get("/activity/alerts/stream", s.streamAlertActivity)
		r.Get("/activity/deploys", s.listDeployActivity)
		r.Get("/activity/deploys/stream", s.streamDeployActivity)
	})

	// Platform-wide audit log endpoints. Mounted at /audit so callers
	// don't confuse them with the saga-scoped /flows endpoints.
	r.Route("/audit", func(r chi.Router) {
		r.Get("/events", s.listAuditEvents)
		r.Get("/events/{id}", s.getAuditEvent)
		r.Post("/replay", s.replayAudit)
	})

	s.router = r
}

// --- Audit endpoints ------------------------------------------------------

// listAuditEvents answers GET /audit/events with filters:
//
//	event_type, domain, service, resource_id, org_id, actor_id,
//	from (RFC3339), to (RFC3339), limit, offset.
func (s *Server) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	if s.recorder == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit recorder not enabled"})
		return
	}
	q := r.URL.Query()
	filter := postgres.AuditFilter{
		EventType:      q.Get("event_type"),
		Domain:         q.Get("domain"),
		SourceService:  q.Get("service"),
		ResourceID:     q.Get("resource_id"),
		OrganizationID: q.Get("org_id"),
		ActorID:        q.Get("actor_id"),
	}
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.From = &t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			filter.To = &t
		}
	}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Limit = n
		}
	}
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			filter.Offset = n
		}
	}
	rows, err := s.recorder.ListAudit(r.Context(), filter)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": rows, "count": len(rows)})
}

// getAuditEvent answers GET /audit/events/{id}.
func (s *Server) getAuditEvent(w http.ResponseWriter, r *http.Request) {
	if s.recorder == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit recorder not enabled"})
		return
	}
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	row, err := s.recorder.GetAudit(r.Context(), id)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, row)
}

// replayAudit answers POST /audit/replay with a {"events":[...]} body.
// Admin-only: the BFF / gateway is expected to gate this route.
func (s *Server) replayAudit(w http.ResponseWriter, r *http.Request) {
	if s.recorder == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "audit recorder not enabled"})
		return
	}
	var req usecase.ReplayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	n, err := s.recorder.ReplayBatch(r.Context(), req)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replayed": n})
}

func (s *Server) listFlows(w http.ResponseWriter, r *http.Request) {
	filter := postgres.ListFlowsFilter{}
	switch r.URL.Query().Get("state") {
	case "active":
		filter.State = postgres.FilterActive
	case "completed":
		filter.State = postgres.FilterCompleted
	case "failed":
		filter.State = postgres.FilterFailed
	}
	if k := r.URL.Query().Get("kind"); k != "" {
		filter.Kind = domain.FlowKind(k)
	}
	flows, err := s.tracker.ListFlows(r.Context(), filter)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flowsJSON(flows)})
}

func (s *Server) listActiveFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := s.tracker.ListFlows(r.Context(), postgres.ListFlowsFilter{State: postgres.FilterActive})
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flowsJSON(flows)})
}

func (s *Server) flowStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.tracker.Stats(r.Context())
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// pulseSummary answers GET /api/v1/pulse/summary?org_id={id} with the
// federated §3 snapshot. Returns 503 when the aggregator isn't
// configured so operators see the honest "no downstream URLs" answer
// instead of an empty snapshot that looks healthy.
func (s *Server) pulseSummary(w http.ResponseWriter, r *http.Request) {
	if s.aggregator == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "pulse aggregator not configured",
		})
		return
	}
	orgID := r.URL.Query().Get("org_id")
	if orgID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "org_id is required"})
		return
	}
	snap, err := s.aggregator.GetSnapshot(r.Context(), orgID)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) getFlow(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "flowID"))
	if err != nil {
		httpError(w, err)
		return
	}
	flow, err := s.tracker.GetFlow(r.Context(), id)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, flow)
}

func (s *Server) replayFlow(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "flowID"))
	if err != nil {
		httpError(w, err)
		return
	}
	count, err := s.tracker.Replay(r.Context(), id)
	if err != nil {
		httpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replayed": count})
}

// streamFlows upgrades to a WebSocket and pushes each flow update as JSON.
func (s *Server) streamFlows(w http.ResponseWriter, r *http.Request) {
	conn, err := s.wsUp.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	subID, ch := s.tracker.Subscribe()
	defer s.tracker.Unsubscribe(subID)

	// Reader goroutine to detect client disconnect.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case flow, ok := <-ch:
			if !ok {
				return
			}
			if err := conn.WriteJSON(flowJSON(flow)); err != nil {
				return
			}
		}
	}
}

// --- JSON helpers ---------------------------------------------------------

// flowJSON converts the domain Flow to the public API shape documented in
// the §19 task description. Steps are omitted from the summary for brevity
// (they're fetched via GET /flows/{id}).
func flowJSON(f *domain.Flow) map[string]any {
	return map[string]any{
		"id":             f.ID,
		"kind":           f.Kind,
		"state":          f.State,
		"trigger_event":  f.TriggerEvent,
		"current_step":   f.CurrentStep,
		"services":       f.Services,
		"started_at":     f.StartedAt,
		"completed_at":   f.CompletedAt,
		"steps_complete": f.StepsComplete,
		"steps_total":    f.StepsTotal,
		"duration_ms":    f.DurationMS,
		"error":          f.Error,
	}
}

func flowsJSON(flows []domain.Flow) []map[string]any {
	out := make([]map[string]any, 0, len(flows))
	for i := range flows {
		out = append(out, flowJSON(&flows[i]))
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func httpError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrFlowNotFound), errors.Is(err, domain.ErrAuditNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

// --- §3.1/§3.2 activity streams -------------------------------------------

// listAlertActivity returns the current in-memory alert activity
// snapshot. Mostly useful for warm-starts before subscribing to the
// live stream.
func (s *Server) listAlertActivity(w http.ResponseWriter, r *http.Request) {
	if s.alertTracker == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "alert tracker not configured"})
		return
	}
	org := r.URL.Query().Get("org_id")
	snap := s.alertTracker.Snapshot()
	if org != "" {
		filtered := snap[:0]
		for _, e := range snap {
			if e.OrgID == "" || e.OrgID == org {
				filtered = append(filtered, e)
			}
		}
		snap = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": snap})
}

// streamAlertActivity upgrades to WebSocket and pushes each alert
// activity update as JSON. Matches the flow streaming pattern so the
// BFF can reuse its existing gorilla/websocket plumbing.
func (s *Server) streamAlertActivity(w http.ResponseWriter, r *http.Request) {
	if s.alertTracker == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "alert tracker not configured"})
		return
	}
	org := r.URL.Query().Get("org_id")

	conn, err := s.wsUp.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("alert ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	subID, ch := s.alertTracker.Subscribe()
	defer s.alertTracker.Unsubscribe(subID)

	// Push the current snapshot first so subscribers don't stare at an
	// empty strip for the first live update.
	for _, e := range s.alertTracker.Snapshot() {
		if org != "" && e.OrgID != "" && e.OrgID != org {
			continue
		}
		if err := conn.WriteJSON(e); err != nil {
			return
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}
			if org != "" && entry.OrgID != "" && entry.OrgID != org {
				continue
			}
			if err := conn.WriteJSON(entry); err != nil {
				return
			}
		}
	}
}

// listDeployActivity returns the current in-memory deploy activity
// snapshot.
func (s *Server) listDeployActivity(w http.ResponseWriter, r *http.Request) {
	if s.deployTracker == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "deploy tracker not configured"})
		return
	}
	org := r.URL.Query().Get("org_id")
	snap := s.deployTracker.Snapshot()
	if org != "" {
		filtered := snap[:0]
		for _, e := range snap {
			if e.OrgID == "" || e.OrgID == org {
				filtered = append(filtered, e)
			}
		}
		snap = filtered
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": snap})
}

// streamDeployActivity is the deploy-side mirror of streamAlertActivity.
func (s *Server) streamDeployActivity(w http.ResponseWriter, r *http.Request) {
	if s.deployTracker == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "deploy tracker not configured"})
		return
	}
	org := r.URL.Query().Get("org_id")

	conn, err := s.wsUp.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("deploy ws upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	subID, ch := s.deployTracker.Subscribe()
	defer s.deployTracker.Unsubscribe(subID)

	for _, e := range s.deployTracker.Snapshot() {
		if org != "" && e.OrgID != "" && e.OrgID != org {
			continue
		}
		if err := conn.WriteJSON(e); err != nil {
			return
		}
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-done:
			return
		case entry, ok := <-ch:
			if !ok {
				return
			}
			if org != "" && entry.OrgID != "" && entry.OrgID != org {
				continue
			}
			if err := conn.WriteJSON(entry); err != nil {
				return
			}
		}
	}
}
