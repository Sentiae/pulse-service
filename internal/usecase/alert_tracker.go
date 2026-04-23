package usecase

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sentiae/pulse-service/internal/domain"
	"github.com/sentiae/pulse-service/pkg/events"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// AlertTracker consumes ops.alert.* CloudEvents and maintains an
// in-memory projection keyed by alert id so live subscribers get a
// compact feed of firing / acknowledged / resolved transitions.
//
// Retention is intentionally short — resolved alerts drop out of the
// ring buffer after retention so the landing strip doesn't become a
// graveyard. The authoritative alert record lives in ops-service.
type AlertTracker struct {
	mu sync.RWMutex

	// entries is keyed by alert_id. Firing alerts are kept indefinitely;
	// acknowledged/resolved alerts are tombstoned with UpdatedAt and
	// evicted by the next broadcast after retention.
	entries   map[string]*domain.AlertActivityEntry
	order     []string
	retention time.Duration
	maxSize   int

	// Fan-out to subscribers.
	subscribers map[int]chan *domain.AlertActivityEntry
	nextSubID   int
}

// NewAlertTracker builds an alert tracker with a 10-minute retention
// window for terminal alerts and a 200-entry ring buffer.
func NewAlertTracker() *AlertTracker {
	return &AlertTracker{
		entries:     make(map[string]*domain.AlertActivityEntry),
		subscribers: make(map[int]chan *domain.AlertActivityEntry),
		retention:   10 * time.Minute,
		maxSize:     200,
	}
}

// OnEvent is invoked by the Kafka consumer for each ops.alert.* event.
// It is idempotent: repeated deliveries of the same event do not
// double-add or double-broadcast.
func (t *AlertTracker) OnEvent(ctx context.Context, event events.CloudEvent) error {
	alertID, orgID, payload := extractAlertFields(event.Data)
	if alertID == "" {
		return nil
	}

	now := parseEventTime(event.Time)
	status := statusFromAlertEvent(event.Type)

	t.mu.Lock()
	entry, exists := t.entries[alertID]
	if !exists {
		entry = &domain.AlertActivityEntry{
			ID:        alertID,
			OrgID:     orgID,
			StartedAt: now,
		}
		t.entries[alertID] = entry
		t.order = append(t.order, alertID)
		// Enforce ring-buffer cap — drop oldest non-firing entries first.
		if len(t.order) > t.maxSize {
			dropped := t.order[0]
			t.order = t.order[1:]
			delete(t.entries, dropped)
		}
	}

	// Merge fields from the incoming event. CloudEvent payloads for
	// ops.alert.* don't carry all fields on every transition (e.g.
	// acknowledged only repeats severity/name), so we union instead of
	// overwriting.
	if payload.Severity != "" {
		entry.Severity = payload.Severity
	}
	if payload.Name != "" {
		entry.Summary = payload.Name
	}
	if payload.Description != "" && entry.Summary == "" {
		entry.Summary = payload.Description
	}
	if payload.Service != "" {
		entry.ServiceName = payload.Service
	}
	if payload.ServiceID != "" {
		entry.ServiceID = payload.ServiceID
	}
	if orgID != "" {
		entry.OrgID = orgID
	}
	if status != "" {
		entry.Status = status
	}
	entry.UpdatedAt = now

	// Evict terminal entries older than retention before broadcasting.
	t.gcLocked(now)

	snapshot := *entry
	t.mu.Unlock()

	t.broadcast(&snapshot)
	return nil
}

// gcLocked evicts resolved/acknowledged entries older than retention.
// Must be called with t.mu held.
func (t *AlertTracker) gcLocked(now time.Time) {
	if t.retention <= 0 {
		return
	}
	cutoff := now.Add(-t.retention)
	kept := t.order[:0]
	for _, id := range t.order {
		e, ok := t.entries[id]
		if !ok {
			continue
		}
		if e.Status == "resolved" && e.UpdatedAt.Before(cutoff) {
			delete(t.entries, id)
			continue
		}
		kept = append(kept, id)
	}
	t.order = kept
}

// Subscribe returns a channel of activity updates and an id used to
// unsubscribe.
func (t *AlertTracker) Subscribe() (int, <-chan *domain.AlertActivityEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.nextSubID
	t.nextSubID++
	ch := make(chan *domain.AlertActivityEntry, 64)
	t.subscribers[id] = ch
	return id, ch
}

// Unsubscribe closes the subscriber channel and removes it.
func (t *AlertTracker) Unsubscribe(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.subscribers[id]; ok {
		close(ch)
		delete(t.subscribers, id)
	}
}

// Snapshot returns the currently-tracked activity entries, ordered by
// most recent update.
func (t *AlertTracker) Snapshot() []*domain.AlertActivityEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*domain.AlertActivityEntry, 0, len(t.order))
	for _, id := range t.order {
		if e, ok := t.entries[id]; ok {
			clone := *e
			out = append(out, &clone)
		}
	}
	return out
}

func (t *AlertTracker) broadcast(entry *domain.AlertActivityEntry) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, ch := range t.subscribers {
		select {
		case ch <- entry:
		default:
			// Slow consumer — drop. Live-strip is cosmetic.
		}
	}
}

// alertPayload is the slice of ops.alert.* metadata we care about.
type alertPayload struct {
	Name        string
	Severity    string
	Description string
	Service     string
	ServiceID   string
}

// extractAlertFields digs the alert_id + useful fields out of the
// CloudEvent data envelope. Ops-service puts alert_id in ResourceID on
// ack/resolve and in Metadata.alert_id on trigger; we handle both.
func extractAlertFields(raw []byte) (alertID, orgID string, payload alertPayload) {
	var envelope struct {
		ResourceID     string         `json:"resource_id"`
		OrganizationID string         `json:"organization_id"`
		Metadata       map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", "", alertPayload{}
	}
	orgID = envelope.OrganizationID
	if envelope.Metadata != nil {
		payload.Name = stringField(envelope.Metadata, "name")
		payload.Severity = stringField(envelope.Metadata, "severity")
		payload.Description = stringField(envelope.Metadata, "description")
		payload.Service = stringField(envelope.Metadata, "service", "service_name")
		payload.ServiceID = stringField(envelope.Metadata, "service_id")
		alertID = stringField(envelope.Metadata, "alert_id")
	}
	if alertID == "" {
		alertID = envelope.ResourceID
	}
	return alertID, orgID, payload
}

// statusFromAlertEvent maps a CloudEvent type to the normalized
// AlertActivityEntry.Status value.
func statusFromAlertEvent(eventType string) string {
	switch eventType {
	case "ops.alert.triggered", "ops.alert.fired":
		return "firing"
	case "ops.alert.acknowledged":
		return "acknowledged"
	case "ops.alert.resolved":
		return "resolved"
	default:
		return ""
	}
}

// Ensure the usecase package has access to the logger pkg so gc failures
// are surfaced when running inline. Not currently used but kept here so
// follow-up debug hooks don't need an import shuffle.
var _ = logger.Info
