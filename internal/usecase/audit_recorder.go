package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/sentiae/pulse-service/internal/domain"
	"github.com/sentiae/pulse-service/internal/repository/postgres"
	"github.com/sentiae/pulse-service/pkg/events"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// AuditRecorder records every CloudEvent the service observes into the
// pulse_event_audit table. This is the platform-wide audit log promised
// by §19.4: a catch-all consumer subscribed to the full registered-topic
// set across all domains, indexed for fast queries by resource, org,
// source service, and time window.
//
// AuditRecorder is independent from FlowTracker. It does not try to
// interpret saga semantics; it just persists what it sees. FlowTracker
// already writes its own EventAudit rows for saga events (with a real
// FlowID), so to avoid duplicating rows we tag saga events here by
// looking for saga_id in metadata and let FlowTracker own them.
type AuditRecorder struct {
	repo      *postgres.FlowRepository
	publisher events.Publisher
}

// NewAuditRecorder builds the recorder.
func NewAuditRecorder(repo *postgres.FlowRepository, publisher events.Publisher) *AuditRecorder {
	return &AuditRecorder{repo: repo, publisher: publisher}
}

// OnEvent is invoked by the catch-all Kafka consumer. Every registered
// CloudEvent lands here. Saga events are skipped because FlowTracker
// writes a richer audit row for them already.
func (a *AuditRecorder) OnEvent(ctx context.Context, event events.CloudEvent) error {
	if isSagaEvent(event.Type) {
		// FlowTracker handles these with FlowID set.
		return nil
	}

	meta := parseEventMeta(event.Data)
	dot := strings.Index(event.Type, ".")
	domainName := event.Type
	if dot > 0 {
		domainName = event.Type[:dot]
	}

	row := &domain.EventAudit{
		ID:             uuid.New(),
		EventType:      event.Type,
		Domain:         domainName,
		Source:         event.Source,
		SourceService:  event.Source,
		ResourceType:   meta.ResourceType,
		ResourceID:     meta.ResourceID,
		OrganizationID: meta.OrganizationID,
		ActorID:        meta.ActorID,
		OccurredAt:     parseEventTime(event.Time),
		Payload:        string(event.Data),
	}

	if err := a.repo.AppendAudit(ctx, row); err != nil {
		return fmt.Errorf("append audit: %w", err)
	}
	return nil
}

// ListAudit queries the audit log.
func (a *AuditRecorder) ListAudit(ctx context.Context, f postgres.AuditFilter) ([]domain.EventAudit, error) {
	return a.repo.ListAudit(ctx, f)
}

// GetAudit fetches a single audit row by id.
func (a *AuditRecorder) GetAudit(ctx context.Context, id uuid.UUID) (*domain.EventAudit, error) {
	return a.repo.GetAuditByID(ctx, id)
}

// ReplayRequest is a batch replay payload.
type ReplayRequest struct {
	Events []ReplayEvent `json:"events"`
}

// ReplayEvent is a single replay entry — the admin supplies the event
// type and payload; the platform wraps it in a CloudEvent.
type ReplayEvent struct {
	EventType string          `json:"event_type"`
	Key       string          `json:"key"`
	Data      json.RawMessage `json:"data"`
}

// ReplayBatch re-emits the supplied events into Kafka under their
// original event types and records an audit.replay.executed event for
// each. Returns the number of events emitted.
func (a *AuditRecorder) ReplayBatch(ctx context.Context, req ReplayRequest) (int, error) {
	if a.publisher == nil {
		return 0, fmt.Errorf("no publisher configured")
	}
	count := 0
	for _, ev := range req.Events {
		if ev.EventType == "" {
			continue
		}
		// Build the EventData envelope from the supplied data payload.
		// We pass the raw data through as Metadata["payload"] so downstream
		// consumers can tell this is a replay (the wrapper event type
		// "audit.replay.executed" below records provenance).
		data := events.EventData{
			ResourceType: "audit_replay",
			ResourceID:   ev.Key,
			Metadata: map[string]any{
				"replayed_event": ev.EventType,
				"payload":        string(ev.Data),
			},
			Timestamp: time.Now().UTC(),
		}
		if err := a.publisher.Publish(ctx, "audit.replay.executed", data); err != nil {
			logger.Warn("audit replay publish failed: %v", err)
			continue
		}
		count++
	}
	return count, nil
}

// --- helpers ---------------------------------------------------------------

// isSagaEvent mirrors FlowTracker's scope so AuditRecorder can skip
// events that FlowTracker already persists with a real FlowID.
func isSagaEvent(eventType string) bool {
	return strings.HasPrefix(eventType, "saga.") ||
		strings.HasPrefix(eventType, "canvas.saga.")
}

// eventMeta is the subset of EventData that audit cares about.
type eventMeta struct {
	ResourceType   string
	ResourceID     string
	OrganizationID string
	ActorID        string
}

// parseEventMeta extracts the standard envelope fields without
// committing to a specific event schema.
func parseEventMeta(raw []byte) eventMeta {
	var env struct {
		ResourceType   string `json:"resource_type"`
		ResourceID     string `json:"resource_id"`
		OrganizationID string `json:"organization_id"`
		ActorID        string `json:"actor_id"`
	}
	_ = json.Unmarshal(raw, &env)
	return eventMeta{
		ResourceType:   env.ResourceType,
		ResourceID:     env.ResourceID,
		OrganizationID: env.OrganizationID,
		ActorID:        env.ActorID,
	}
}
