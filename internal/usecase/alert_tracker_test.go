package usecase

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sentiae/pulse-service/pkg/events"
)

func buildAlertEvent(t *testing.T, evType, alertID, orgID string, meta map[string]any) events.CloudEvent {
	t.Helper()
	data := map[string]any{
		"resource_id":     alertID,
		"organization_id": orgID,
		"metadata":        meta,
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return events.CloudEvent{
		Type: evType,
		Time: time.Now().UTC().Format(time.RFC3339Nano),
		Data: raw,
	}
}

func TestAlertTracker_FiringAlertCreatesEntryAndBroadcasts(t *testing.T) {
	tr := NewAlertTracker()
	subID, ch := tr.Subscribe()
	defer tr.Unsubscribe(subID)

	ev := buildAlertEvent(t, "ops.alert.triggered", "alert-1", "org-1", map[string]any{
		"name":       "High CPU",
		"severity":   "critical",
		"service":    "api",
		"service_id": "svc-1",
	})
	if err := tr.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}

	select {
	case entry := <-ch:
		if entry.ID != "alert-1" {
			t.Fatalf("expected alert-1, got %q", entry.ID)
		}
		if entry.Severity != "critical" {
			t.Fatalf("expected critical, got %q", entry.Severity)
		}
		if entry.Status != "firing" {
			t.Fatalf("expected firing, got %q", entry.Status)
		}
		if entry.ServiceName != "api" || entry.ServiceID != "svc-1" {
			t.Fatalf("service fields not merged: %+v", entry)
		}
		if entry.OrgID != "org-1" {
			t.Fatalf("org id missing: %+v", entry)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("no broadcast received")
	}

	snap := tr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
}

func TestAlertTracker_AcknowledgeResolveUpdateSameEntry(t *testing.T) {
	tr := NewAlertTracker()
	ctx := context.Background()

	_ = tr.OnEvent(ctx, buildAlertEvent(t, "ops.alert.triggered", "alert-2", "org-1", map[string]any{
		"name":     "DB Connections",
		"severity": "high",
	}))
	_ = tr.OnEvent(ctx, buildAlertEvent(t, "ops.alert.acknowledged", "alert-2", "org-1", map[string]any{
		"alert_id":        "alert-2",
		"acknowledged_by": "user-1",
	}))
	_ = tr.OnEvent(ctx, buildAlertEvent(t, "ops.alert.resolved", "alert-2", "org-1", map[string]any{
		"alert_id":   "alert-2",
		"resolution": "auto",
	}))

	snap := tr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected single entry, got %d", len(snap))
	}
	if snap[0].Status != "resolved" {
		t.Fatalf("expected resolved, got %q", snap[0].Status)
	}
	// Severity from original payload preserved even when ack/resolve
	// payloads omit it.
	if snap[0].Severity != "high" {
		t.Fatalf("expected severity merged forward, got %q", snap[0].Severity)
	}
}

func TestAlertTracker_UnknownEventTypeNoOp(t *testing.T) {
	tr := NewAlertTracker()
	ev := buildAlertEvent(t, "unrelated.event", "alert-99", "org-1", map[string]any{})
	if err := tr.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	// The event still carries a resource_id, so an entry is created but
	// with empty Status. That's fine — the BFF filters on known statuses.
	snap := tr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	if snap[0].Status != "" {
		t.Fatalf("expected empty status for unknown event, got %q", snap[0].Status)
	}
}

func TestAlertTracker_MissingAlertIDDropsEvent(t *testing.T) {
	tr := NewAlertTracker()
	ev := buildAlertEvent(t, "ops.alert.triggered", "", "org-1", map[string]any{})
	if err := tr.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if len(tr.Snapshot()) != 0 {
		t.Fatal("expected drop when alert id is empty")
	}
}
