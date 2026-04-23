package usecase

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sentiae/pulse-service/pkg/events"
)

func buildDeployEvent(t *testing.T, evType, deployID, orgID string, meta map[string]any) events.CloudEvent {
	t.Helper()
	data := map[string]any{
		"resource_id":     deployID,
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

func TestDeployTracker_LifecycleTransitions(t *testing.T) {
	tr := NewDeployTracker()
	ctx := context.Background()

	_ = tr.OnEvent(ctx, buildDeployEvent(t, "ops.deployment.started", "dep-1", "org-1", map[string]any{
		"service":     "api",
		"environment": "production",
		"strategy":    "rolling",
	}))
	_ = tr.OnEvent(ctx, buildDeployEvent(t, "ops.deploy.in_progress", "dep-1", "org-1", map[string]any{
		"rollout_percent": float64(50),
	}))
	_ = tr.OnEvent(ctx, buildDeployEvent(t, "ops.deployment.completed", "dep-1", "org-1", map[string]any{}))

	snap := tr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 deploy entry, got %d", len(snap))
	}
	e := snap[0]
	if e.Status != "succeeded" {
		t.Fatalf("expected succeeded, got %q", e.Status)
	}
	if e.ProgressPct != 100 {
		t.Fatalf("expected 100%% progress, got %d", e.ProgressPct)
	}
	if e.ServiceName != "api" || e.Environment != "production" || e.Strategy != "rolling" {
		t.Fatalf("metadata merge lost fields: %+v", e)
	}
}

func TestDeployTracker_FailureCapturesStatus(t *testing.T) {
	tr := NewDeployTracker()
	_ = tr.OnEvent(context.Background(), buildDeployEvent(t, "ops.deployment.failed", "dep-2", "org-2", map[string]any{
		"service":     "billing",
		"environment": "staging",
	}))
	snap := tr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(snap))
	}
	if snap[0].Status != "failed" {
		t.Fatalf("expected failed, got %q", snap[0].Status)
	}
	if snap[0].ServiceName != "billing" {
		t.Fatalf("service missing: %+v", snap[0])
	}
}

func TestDeployTracker_BroadcastsEachUpdate(t *testing.T) {
	tr := NewDeployTracker()
	id, ch := tr.Subscribe()
	defer tr.Unsubscribe(id)

	_ = tr.OnEvent(context.Background(), buildDeployEvent(t, "ops.deployment.started", "dep-3", "org-3", map[string]any{
		"service": "web",
	}))
	_ = tr.OnEvent(context.Background(), buildDeployEvent(t, "ops.deployment.completed", "dep-3", "org-3", map[string]any{}))

	seen := 0
	for seen < 2 {
		select {
		case e := <-ch:
			if e == nil {
				t.Fatal("nil entry")
			}
			seen++
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected 2 broadcasts, got %d", seen)
		}
	}
}

func TestDeployTracker_UnknownDeployIDDropped(t *testing.T) {
	tr := NewDeployTracker()
	ev := buildDeployEvent(t, "ops.deployment.started", "", "org-x", map[string]any{})
	if err := tr.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}
	if len(tr.Snapshot()) != 0 {
		t.Fatal("expected drop for missing deployment_id")
	}
}
