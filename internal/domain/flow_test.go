package domain

import "testing"

func TestFlowKindFromEventType(t *testing.T) {
	cases := []struct {
		in   string
		want FlowKind
	}{
		{"saga.code_test_deploy.started", FlowKindCodeTestDeploy},
		{"saga.spec_driven.completed", FlowKindSpecDriven},
		{"saga.self_healing.diagnosed", FlowKindSelfHealing},
		{"saga.nl_query.failed", FlowKindNLQuery},
		{"canvas.saga.import_analyze_canvas.nodes_created", FlowKindImportAnalyzeCanvas},
		{"git.push.created", FlowKindUnknown},
		{"", FlowKindUnknown},
	}
	for _, c := range cases {
		if got := FlowKindFromEventType(c.in); got != c.want {
			t.Errorf("FlowKindFromEventType(%q) = %v want %v", c.in, got, c.want)
		}
	}
}

func TestIsStart(t *testing.T) {
	if !IsStart("saga.code_test_deploy.started") {
		t.Error("expected started event to be start")
	}
	if IsStart("saga.code_test_deploy.completed") {
		t.Error("completed must not be a start")
	}
}

func TestIsTerminal(t *testing.T) {
	terminal, state := IsTerminal("saga.code_test_deploy.completed")
	if !terminal || state != FlowStateCompleted {
		t.Errorf("expected completed terminal, got terminal=%v state=%v", terminal, state)
	}
	terminal, state = IsTerminal("saga.spec_driven.failed")
	if !terminal || state != FlowStateFailed {
		t.Errorf("expected failed terminal, got terminal=%v state=%v", terminal, state)
	}
	terminal, state = IsTerminal("saga.self_healing.rolled_back")
	if !terminal || state != FlowStateFailed {
		t.Errorf("rolled_back should map to failed terminal, got state=%v", state)
	}
	terminal, _ = IsTerminal("saga.code_test_deploy.tests_requested")
	if terminal {
		t.Error("intermediate events must not be terminal")
	}
}

func TestStepNameFromEventType(t *testing.T) {
	if got := StepNameFromEventType("saga.code_test_deploy.tests_completed"); got != "tests_completed" {
		t.Errorf("got %q", got)
	}
	if got := StepNameFromEventType("no_dots"); got != "no_dots" {
		t.Errorf("got %q", got)
	}
}

func TestServiceForEvent(t *testing.T) {
	if got := ServiceForEvent("saga.spec_driven.canvas_created"); got != "canvas" {
		t.Errorf("canvas_created service = %q", got)
	}
	if got := ServiceForEvent("saga.code_test_deploy.deployment_requested"); got != "ops" {
		t.Errorf("deployment_requested service = %q", got)
	}
	if got := ServiceForEvent("saga.nl_query.executed"); got != "data" {
		t.Errorf("nl_query.executed service = %q", got)
	}
}
