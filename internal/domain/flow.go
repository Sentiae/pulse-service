package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

// FlowKind enumerates the canonical in-flight flow categories Pulse tracks.
// Each kind corresponds to a saga in another service (work, foundry, canvas,
// ops). See docs/§19.4 for the full list.
type FlowKind string

const (
	FlowKindCodeTestDeploy      FlowKind = "code_test_deploy"
	FlowKindSpecDriven          FlowKind = "spec_driven"
	FlowKindSelfHealing         FlowKind = "self_healing"
	FlowKindNLQuery             FlowKind = "nl_query"
	FlowKindImportAnalyzeCanvas FlowKind = "import_analyze_canvas"
	// FlowKindSpecShipping surfaces the short "spec transitioned to
	// shipped" event so the Pulse activity feed shows specs in flight
	// without inventing a shape.
	FlowKindSpecShipping FlowKind = "spec_shipping"
	FlowKindUnknown      FlowKind = "unknown"
)

// FlowState is the coarse lifecycle state derived from observed events.
type FlowState string

const (
	FlowStateRunning   FlowState = "running"
	FlowStateCompleted FlowState = "completed"
	FlowStateFailed    FlowState = "failed"
)

// Errors exported for the handler layer.
var (
	ErrFlowNotFound  = errors.New("flow not found")
	ErrAuditNotFound = errors.New("audit event not found")
)

// Flow is a single in-flight (or completed) cross-service flow.
//
// A Flow is created when we observe a "saga.<kind>.started" event. It is
// subsequently updated as we see matching step/completed/failed events for
// the same saga_id.
type Flow struct {
	ID             uuid.UUID  `gorm:"type:uuid;primaryKey" json:"id"`
	SagaID         string     `gorm:"uniqueIndex;size:128;not null" json:"saga_id"`
	Kind           FlowKind   `gorm:"size:64;not null;index" json:"kind"`
	State          FlowState  `gorm:"size:32;not null;index" json:"state"`
	TriggerEvent   string     `gorm:"size:128;not null" json:"trigger_event"`
	OrganizationID string     `gorm:"size:64;index" json:"organization_id,omitempty"`
	UserID         string     `gorm:"size:64;index" json:"user_id,omitempty"`
	CurrentStep    string     `gorm:"size:128" json:"current_step,omitempty"`
	Services       StringList `gorm:"serializer:json" json:"services"`
	StepsComplete  int        `json:"steps_complete"`
	StepsTotal     int        `json:"steps_total"`
	StartedAt      time.Time  `gorm:"not null" json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	DurationMS     int64      `json:"duration_ms"`
	Error          string     `gorm:"type:text" json:"error,omitempty"`
	Steps          []FlowStep `gorm:"foreignKey:FlowID;references:ID" json:"steps,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// TableName overrides the GORM default to keep it stable.
func (Flow) TableName() string { return "pulse_flows" }

// StringList is a convenience type persisted as a JSON array via GORM's
// serializer:json tag.
type StringList []string

// ExpectedSteps is a best-effort hint of the number of steps a given saga
// kind produces. Used to populate StepsTotal when a flow starts so the UI
// can show progress even before the later steps land.
func (k FlowKind) ExpectedSteps() int {
	switch k {
	case FlowKindCodeTestDeploy:
		return 5 // started, tests_requested, tests_completed, gate_evaluated, deployment_requested/completed
	case FlowKindSpecDriven:
		return 7
	case FlowKindSelfHealing:
		return 6
	case FlowKindNLQuery:
		return 4
	case FlowKindImportAnalyzeCanvas:
		return 4
	case FlowKindSpecShipping:
		return 1
	default:
		return 0
	}
}

// FlowKindFromEventType maps a bare event type to the flow kind it belongs
// to, or FlowKindUnknown if the event is not a saga event we track.
func FlowKindFromEventType(eventType string) FlowKind {
	switch {
	case startsWith(eventType, "saga.code_test_deploy."):
		return FlowKindCodeTestDeploy
	case startsWith(eventType, "saga.spec_driven."):
		return FlowKindSpecDriven
	case startsWith(eventType, "saga.self_healing."):
		return FlowKindSelfHealing
	case startsWith(eventType, "saga.nl_query."):
		return FlowKindNLQuery
	case startsWith(eventType, "canvas.saga.import_analyze_canvas."):
		return FlowKindImportAnalyzeCanvas
	case startsWith(eventType, "saga.spec_shipping."):
		return FlowKindSpecShipping
	default:
		return FlowKindUnknown
	}
}

// IsStart returns true if the bare event type is the "started" event of
// some saga.
func IsStart(eventType string) bool {
	return endsWith(eventType, ".started")
}

// IsTerminal returns (true, state) if the event is a terminal event for the
// saga (completed or failed). Otherwise returns (false, "").
func IsTerminal(eventType string) (bool, FlowState) {
	switch {
	case endsWith(eventType, ".completed"):
		return true, FlowStateCompleted
	case endsWith(eventType, ".failed"),
		endsWith(eventType, ".rolled_back"):
		return true, FlowStateFailed
	}
	return false, ""
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}
