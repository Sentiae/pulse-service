package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/sentiae/pulse-service/internal/domain"
	"github.com/sentiae/pulse-service/internal/repository/postgres"
	"github.com/sentiae/pulse-service/pkg/events"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// FlowTracker is the heart of pulse-service: it consumes saga events, uses
// them to build Flow rows, and broadcasts live updates to any WebSocket
// subscribers.
//
// OnEvent is idempotent at the saga level: if we see a started event twice
// we won't create a duplicate flow; if we see a step event for an unknown
// saga we treat it as a late-arriving start and create a synthetic flow.
type FlowTracker struct {
	repo      *postgres.FlowRepository
	publisher events.Publisher

	// Subscribers for live updates (WebSocket). Populated via Subscribe /
	// Unsubscribe; broadcast is best-effort and non-blocking.
	mu          sync.RWMutex
	subscribers map[int]chan *domain.Flow
	nextSubID   int
}

// NewFlowTracker constructs the tracker.
func NewFlowTracker(repo *postgres.FlowRepository, publisher events.Publisher) *FlowTracker {
	return &FlowTracker{
		repo:        repo,
		publisher:   publisher,
		subscribers: make(map[int]chan *domain.Flow),
	}
}

// OnEvent is invoked by the Kafka consumer for every saga event Pulse
// subscribes to. It is safe to call concurrently for different sagas; for
// the same saga_id, the Kafka consumer's single-goroutine-per-partition
// model gives us serial delivery which keeps our state consistent without
// locks.
func (t *FlowTracker) OnEvent(ctx context.Context, event events.CloudEvent) error {
	kind := domain.FlowKindFromEventType(event.Type)
	if kind == domain.FlowKindUnknown {
		return nil
	}

	// Extract saga_id from the CloudEvent data. We don't deserialize into a
	// typed struct because different sagas use different schemas.
	sagaID, orgID, userID, errMsg := extractIDs(event.Data)
	if sagaID == "" {
		// Fallback: some sagas use spec_id/saga_id interchangeably. Grab
		// any string field that looks like an id.
		logger.Debug("event %s missing saga_id, skipping", event.Type)
		return nil
	}

	// Always persist raw event for replay.
	rawPayload := string(event.Data)

	flow, err := t.repo.GetFlowBySagaID(ctx, sagaID)
	switch {
	case err == domain.ErrFlowNotFound && domain.IsStart(event.Type):
		// Create a new flow.
		flow = &domain.Flow{
			ID:             uuid.New(),
			SagaID:         sagaID,
			Kind:           kind,
			State:          domain.FlowStateRunning,
			TriggerEvent:   event.Type,
			OrganizationID: orgID,
			UserID:         userID,
			CurrentStep:    domain.StepNameFromEventType(event.Type),
			Services:       domain.StringList{domain.ServiceForEvent(event.Type)},
			StepsComplete:  0,
			StepsTotal:     kind.ExpectedSteps(),
			StartedAt:      parseEventTime(event.Time),
		}
		if err := t.repo.CreateFlow(ctx, flow); err != nil {
			return fmt.Errorf("create flow: %w", err)
		}
		t.recordAudit(ctx, flow.ID, sagaID, event, rawPayload)
		t.emitFlowCreated(ctx, flow)
		t.broadcast(flow)
		return nil

	case err == domain.ErrFlowNotFound:
		// Step event for an unknown saga — create a synthetic flow so we
		// don't drop it on the floor (order-of-arrival tolerance).
		flow = &domain.Flow{
			ID:             uuid.New(),
			SagaID:         sagaID,
			Kind:           kind,
			State:          domain.FlowStateRunning,
			TriggerEvent:   "synthetic:" + event.Type,
			OrganizationID: orgID,
			UserID:         userID,
			Services:       domain.StringList{domain.ServiceForEvent(event.Type)},
			StepsTotal:     kind.ExpectedSteps(),
			StartedAt:      parseEventTime(event.Time),
		}
		if err := t.repo.CreateFlow(ctx, flow); err != nil {
			return fmt.Errorf("create synthetic flow: %w", err)
		}
		t.recordAudit(ctx, flow.ID, sagaID, event, rawPayload)

	case err != nil:
		return fmt.Errorf("lookup flow: %w", err)
	}

	// Existing or synthetic flow — append step and update state.
	stepName := domain.StepNameFromEventType(event.Type)
	now := parseEventTime(event.Time)
	terminal, terminalState := domain.IsTerminal(event.Type)

	step := &domain.FlowStep{
		ID:        uuid.New(),
		FlowID:    flow.ID,
		StepName:  stepName,
		Service:   domain.ServiceForEvent(event.Type),
		EventType: event.Type,
		Status:    domain.FlowStepStatusCompleted,
		StartedAt: now,
		CompletedAt: func() *time.Time {
			n := now
			return &n
		}(),
		Payload: rawPayload,
	}
	if terminal && terminalState == domain.FlowStateFailed {
		step.Status = domain.FlowStepStatusFailed
		step.Error = errMsg
	}
	if err := t.repo.AppendStep(ctx, step); err != nil {
		return fmt.Errorf("append step: %w", err)
	}

	// Update the flow aggregates.
	flow.CurrentStep = stepName
	flow.StepsComplete++
	flow.Services = appendUnique(flow.Services, step.Service)

	if terminal {
		completedAt := now
		flow.CompletedAt = &completedAt
		flow.DurationMS = completedAt.Sub(flow.StartedAt).Milliseconds()
		flow.State = terminalState
		if terminalState == domain.FlowStateFailed {
			flow.Error = errMsg
		}
	}

	if err := t.repo.UpdateFlow(ctx, flow); err != nil {
		return fmt.Errorf("update flow: %w", err)
	}
	t.recordAudit(ctx, flow.ID, sagaID, event, rawPayload)

	if terminal {
		if terminalState == domain.FlowStateCompleted {
			t.emit(ctx, events.EventFlowCompleted, flow)
		} else {
			t.emit(ctx, events.EventFlowFailed, flow)
		}
	} else {
		t.emit(ctx, events.EventFlowStepCompleted, flow)
	}
	t.broadcast(flow)
	return nil
}

// Subscribe returns a channel of live flow updates and a cancel function.
// The channel is buffered so slow consumers are dropped rather than
// blocking the tracker.
func (t *FlowTracker) Subscribe() (int, <-chan *domain.Flow) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.nextSubID
	t.nextSubID++
	ch := make(chan *domain.Flow, 64)
	t.subscribers[id] = ch
	return id, ch
}

func (t *FlowTracker) Unsubscribe(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.subscribers[id]; ok {
		close(ch)
		delete(t.subscribers, id)
	}
}

func (t *FlowTracker) broadcast(flow *domain.Flow) {
	// Copy to avoid races with subsequent mutations in-memory.
	snapshot := *flow
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, ch := range t.subscribers {
		select {
		case ch <- &snapshot:
		default:
			// slow subscriber — drop
		}
	}
}

func (t *FlowTracker) emit(ctx context.Context, eventType string, flow *domain.Flow) {
	if t.publisher == nil {
		return
	}
	data := events.EventData{
		ResourceType: "pulse_flow",
		ResourceID:   flow.ID.String(),
		Metadata: map[string]any{
			"saga_id":        flow.SagaID,
			"kind":           string(flow.Kind),
			"state":          string(flow.State),
			"steps_complete": flow.StepsComplete,
			"steps_total":    flow.StepsTotal,
		},
		Timestamp: time.Now().UTC(),
	}
	if err := t.publisher.Publish(ctx, eventType, data); err != nil {
		logger.Warn("publish %s failed: %v", eventType, err)
	}
}

func (t *FlowTracker) emitFlowCreated(ctx context.Context, flow *domain.Flow) {
	t.emit(ctx, events.EventFlowCreated, flow)
}

func (t *FlowTracker) recordAudit(ctx context.Context, flowID uuid.UUID, sagaID string, ev events.CloudEvent, payload string) {
	entry := &domain.EventAudit{
		FlowID:     flowID,
		SagaID:     sagaID,
		EventType:  ev.Type,
		Source:     ev.Source,
		OccurredAt: parseEventTime(ev.Time),
		Payload:    payload,
	}
	if err := t.repo.AppendEventAudit(ctx, entry); err != nil {
		logger.Warn("append audit failed: %v", err)
	}
}

// extractIDs pulls saga_id + contextual ids out of a CloudEvent data blob
// without committing to a specific schema. Looks at EventData.Metadata
// first (the canonical location) and falls back to top-level fields.
func extractIDs(raw []byte) (sagaID, orgID, userID, errMsg string) {
	var envelope struct {
		OrganizationID string         `json:"organization_id"`
		Metadata       map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", "", "", ""
	}
	orgID = envelope.OrganizationID
	if envelope.Metadata != nil {
		sagaID = stringField(envelope.Metadata, "saga_id", "spec_id", "alert_id")
		if userID == "" {
			userID = stringField(envelope.Metadata, "user_id")
		}
		errMsg = stringField(envelope.Metadata, "reason", "error")
	}
	return sagaID, orgID, userID, errMsg
}

func stringField(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	return ""
}

func parseEventTime(ts string) time.Time {
	if ts == "" {
		return time.Now().UTC()
	}
	t, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		return time.Now().UTC()
	}
	return t
}

func appendUnique(existing domain.StringList, v string) domain.StringList {
	if v == "" || v == "unknown" {
		return existing
	}
	for _, e := range existing {
		if e == v {
			return existing
		}
	}
	return append(existing, v)
}

// GetFlow returns a flow by id.
func (t *FlowTracker) GetFlow(ctx context.Context, id uuid.UUID) (*domain.Flow, error) {
	return t.repo.GetFlow(ctx, id)
}

// ListFlows returns flows matching the filter.
func (t *FlowTracker) ListFlows(ctx context.Context, filter postgres.ListFlowsFilter) ([]domain.Flow, error) {
	return t.repo.ListFlows(ctx, filter)
}

// Stats returns an aggregate summary.
func (t *FlowTracker) Stats(ctx context.Context) (*postgres.Stats, error) {
	return t.repo.Stats(ctx)
}

// Replay re-emits every audited event for a flow, back into the Kafka
// publisher (under a replay.* prefix so consumers don't react twice). This
// is the primary Tier-3 debugging lever promised by §19.
func (t *FlowTracker) Replay(ctx context.Context, id uuid.UUID) (int, error) {
	flow, err := t.repo.GetFlow(ctx, id)
	if err != nil {
		return 0, err
	}
	audits, err := t.repo.ListAuditForFlow(ctx, flow.ID)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, a := range audits {
		if t.publisher == nil {
			continue
		}
		data := events.EventData{
			ResourceType: "pulse_flow_replay",
			ResourceID:   flow.ID.String(),
			Metadata: map[string]any{
				"original_event": a.EventType,
				"saga_id":        a.SagaID,
				"occurred_at":    a.OccurredAt,
			},
			Timestamp: time.Now().UTC(),
		}
		// We emit under a synthetic "pulse.flow.replayed" type so no one
		// mistakes it for the original. The original raw payload is in
		// metadata as a string.
		if a.Payload != "" {
			data.Metadata["payload"] = a.Payload
		}
		if err := t.publisher.Publish(ctx, "pulse.flow.replayed", data); err != nil {
			logger.Warn("replay emit failed: %v", err)
			continue
		}
		count++
	}
	return count, nil
}
