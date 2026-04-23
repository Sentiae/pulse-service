package domain

// ServiceForEvent maps an event type to the owning service. This is a rough
// classifier used to populate Flow.Services and FlowStep.Service when we
// don't get the source from the CloudEvent envelope. The mappings mirror
// the event ownership declared in platform-kit/kafka/event_taxonomy.go.
func ServiceForEvent(eventType string) string {
	switch FlowKindFromEventType(eventType) {
	case FlowKindCodeTestDeploy:
		// code_test_deploy saga coordinates work → foundry/canvas → git → runtime
		if contains(eventType, "tests_") {
			return "runtime"
		}
		if contains(eventType, "deployment_") {
			return "ops"
		}
		if contains(eventType, "gate_evaluated") {
			return "ops"
		}
		return "foundry"
	case FlowKindSpecDriven:
		// spec_driven saga moves spec → canvas → foundry → git/runtime
		switch {
		case contains(eventType, "canvas_created"):
			return "canvas"
		case contains(eventType, "session_created"), contains(eventType, "code_written"):
			return "foundry"
		case contains(eventType, "testing_started"):
			return "runtime"
		case contains(eventType, "decomposed"):
			return "work"
		default:
			return "foundry"
		}
	case FlowKindSelfHealing:
		// self_healing saga triggered by ops alerts, driven by foundry
		if contains(eventType, "deployed") || contains(eventType, "rolled_back") {
			return "ops"
		}
		if contains(eventType, "spec_created") {
			return "work"
		}
		return "foundry"
	case FlowKindNLQuery:
		// nl_query executes through data-service with foundry orchestrating
		if contains(eventType, "executed") {
			return "data"
		}
		return "foundry"
	case FlowKindImportAnalyzeCanvas:
		return "canvas"
	default:
		return "unknown"
	}
}

// StepNameFromEventType returns the trailing segment of the bare event type
// as a human-readable step label. E.g. "saga.spec_driven.code_written" →
// "code_written".
func StepNameFromEventType(eventType string) string {
	for i := len(eventType) - 1; i >= 0; i-- {
		if eventType[i] == '.' {
			return eventType[i+1:]
		}
	}
	return eventType
}

func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
