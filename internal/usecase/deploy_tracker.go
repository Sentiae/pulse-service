package usecase

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sentiae/pulse-service/internal/domain"
	"github.com/sentiae/pulse-service/pkg/events"
)

// DeployTracker consumes ops.deployment.*/ops.deploy.* CloudEvents and
// maintains an in-memory projection keyed by deployment id. Completed
// and failed deploys are retained for `retention` so "just shipped"
// cards remain visible on the landing for a few minutes.
type DeployTracker struct {
	mu sync.RWMutex

	entries   map[string]*domain.DeployActivityEntry
	order     []string
	retention time.Duration
	maxSize   int

	subscribers map[int]chan *domain.DeployActivityEntry
	nextSubID   int
}

// NewDeployTracker builds a deploy tracker with a 10-minute retention
// window for terminal deploys and a 200-entry cap.
func NewDeployTracker() *DeployTracker {
	return &DeployTracker{
		entries:     make(map[string]*domain.DeployActivityEntry),
		subscribers: make(map[int]chan *domain.DeployActivityEntry),
		retention:   10 * time.Minute,
		maxSize:     200,
	}
}

// OnEvent updates the projection from a single CloudEvent. Safe for
// concurrent delivery; merges additive fields rather than overwriting
// so partial payloads on progress events don't clobber earlier data.
func (t *DeployTracker) OnEvent(ctx context.Context, event events.CloudEvent) error {
	deployID, orgID, payload := extractDeployFields(event.Data)
	if deployID == "" {
		return nil
	}
	now := parseEventTime(event.Time)
	status, progress := statusAndProgressFromDeployEvent(event.Type, payload)

	t.mu.Lock()
	entry, exists := t.entries[deployID]
	if !exists {
		entry = &domain.DeployActivityEntry{
			ID:        deployID,
			OrgID:     orgID,
			StartedAt: now,
		}
		t.entries[deployID] = entry
		t.order = append(t.order, deployID)
		if len(t.order) > t.maxSize {
			dropped := t.order[0]
			t.order = t.order[1:]
			delete(t.entries, dropped)
		}
	}

	if payload.ServiceName != "" {
		entry.ServiceName = payload.ServiceName
	}
	if payload.ServiceID != "" {
		entry.ServiceID = payload.ServiceID
	}
	if payload.Environment != "" {
		entry.Environment = payload.Environment
	}
	if payload.Strategy != "" {
		entry.Strategy = payload.Strategy
	}
	if orgID != "" {
		entry.OrgID = orgID
	}
	if status != "" {
		entry.Status = status
	}
	if progress > 0 {
		entry.ProgressPct = progress
	} else if status == "succeeded" || status == "completed" {
		entry.ProgressPct = 100
	}
	entry.UpdatedAt = now
	if entry.StartedAt.IsZero() {
		entry.StartedAt = now
	}
	if !entry.StartedAt.IsZero() {
		entry.DurationSeconds = int64(now.Sub(entry.StartedAt).Seconds())
		if entry.DurationSeconds < 0 {
			entry.DurationSeconds = 0
		}
	}

	t.gcLocked(now)
	snapshot := *entry
	t.mu.Unlock()

	t.broadcast(&snapshot)
	return nil
}

func (t *DeployTracker) gcLocked(now time.Time) {
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
		terminal := e.Status == "succeeded" || e.Status == "completed" ||
			e.Status == "failed" || e.Status == "rolled_back"
		if terminal && e.UpdatedAt.Before(cutoff) {
			delete(t.entries, id)
			continue
		}
		kept = append(kept, id)
	}
	t.order = kept
}

// Subscribe returns a channel of activity updates and an id used to unsubscribe.
func (t *DeployTracker) Subscribe() (int, <-chan *domain.DeployActivityEntry) {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.nextSubID
	t.nextSubID++
	ch := make(chan *domain.DeployActivityEntry, 64)
	t.subscribers[id] = ch
	return id, ch
}

// Unsubscribe closes the subscriber channel.
func (t *DeployTracker) Unsubscribe(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.subscribers[id]; ok {
		close(ch)
		delete(t.subscribers, id)
	}
}

// Snapshot returns the currently-tracked entries.
func (t *DeployTracker) Snapshot() []*domain.DeployActivityEntry {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]*domain.DeployActivityEntry, 0, len(t.order))
	for _, id := range t.order {
		if e, ok := t.entries[id]; ok {
			clone := *e
			out = append(out, &clone)
		}
	}
	return out
}

func (t *DeployTracker) broadcast(entry *domain.DeployActivityEntry) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, ch := range t.subscribers {
		select {
		case ch <- entry:
		default:
		}
	}
}

type deployPayload struct {
	ServiceName string
	ServiceID   string
	Environment string
	Strategy    string
	Status      string
	Progress    int
}

func extractDeployFields(raw []byte) (deployID, orgID string, payload deployPayload) {
	var envelope struct {
		ResourceID     string         `json:"resource_id"`
		OrganizationID string         `json:"organization_id"`
		Metadata       map[string]any `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", "", deployPayload{}
	}
	orgID = envelope.OrganizationID
	if envelope.Metadata != nil {
		payload.ServiceName = stringField(envelope.Metadata, "service", "service_name")
		payload.ServiceID = stringField(envelope.Metadata, "service_id")
		payload.Environment = stringField(envelope.Metadata, "environment")
		payload.Strategy = stringField(envelope.Metadata, "strategy")
		payload.Status = stringField(envelope.Metadata, "status")
		if v, ok := envelope.Metadata["rollout_percent"]; ok {
			switch n := v.(type) {
			case float64:
				payload.Progress = int(n)
			case int:
				payload.Progress = n
			}
		}
		if payload.Progress == 0 {
			if v, ok := envelope.Metadata["progress_pct"]; ok {
				if f, okf := v.(float64); okf {
					payload.Progress = int(f)
				}
			}
		}
		deployID = stringField(envelope.Metadata, "deployment_id")
	}
	if deployID == "" {
		deployID = envelope.ResourceID
	}
	return deployID, orgID, payload
}

// statusAndProgressFromDeployEvent maps a CloudEvent type to the
// normalized DeployActivityEntry.Status value plus the expected
// progress floor. For explicit progress events the raw metadata.Progress
// from the payload takes precedence.
func statusAndProgressFromDeployEvent(eventType string, payload deployPayload) (string, int) {
	progress := payload.Progress
	switch eventType {
	case "ops.deployment.started", "ops.deploy.started":
		status := "in_flight"
		if progress == 0 {
			progress = 5
		}
		return status, progress
	case "ops.deploy.in_progress", "ops.deployment.progressed":
		if progress == 0 {
			progress = 50
		}
		return "in_flight", progress
	case "ops.deployment.completed", "ops.deploy.completed":
		return "succeeded", 100
	case "ops.deployment.failed", "ops.deploy.failed":
		return "failed", progress
	case "ops.deployment.rolled_back", "ops.deploy.rolled_back":
		return "rolled_back", progress
	case "ops.deploy.created":
		if progress == 0 {
			progress = 0
		}
		return "pending", progress
	default:
		// Surface whatever status was included in the payload if we can.
		if payload.Status != "" {
			return payload.Status, progress
		}
		return "", progress
	}
}
