package domain

import (
	"time"

	"github.com/google/uuid"
)

// FlowStepStatus enumerates the step lifecycle.
type FlowStepStatus string

const (
	FlowStepStatusRunning   FlowStepStatus = "running"
	FlowStepStatusCompleted FlowStepStatus = "completed"
	FlowStepStatusFailed    FlowStepStatus = "failed"
)

// FlowStep is a single saga transition observed by Pulse.
//
// Each incoming saga event that's not the start event becomes a FlowStep
// attached to its parent Flow.
type FlowStep struct {
	ID          uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	FlowID      uuid.UUID      `gorm:"type:uuid;not null;index" json:"flow_id"`
	StepName    string         `gorm:"size:128;not null" json:"step_name"`
	Service     string         `gorm:"size:64;not null" json:"service"`
	EventType   string         `gorm:"size:128;not null" json:"event_type"`
	Status      FlowStepStatus `gorm:"size:32;not null" json:"status"`
	StartedAt   time.Time      `gorm:"not null" json:"started_at"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	DurationMS  int64          `json:"duration_ms"`
	Error       string         `gorm:"type:text" json:"error,omitempty"`
	Payload     string         `gorm:"type:jsonb" json:"payload,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
}

func (FlowStep) TableName() string { return "pulse_flow_steps" }

// EventAudit persists the raw event sequence used by replay. Each event
// Pulse consumes is stored so POST /flows/{id}/replay can re-emit them in
// order. A separate model from FlowStep because not every observed event
// becomes a step (e.g. duplicate deliveries, malformed payloads).
//
// Since §19.4 we also use EventAudit as the platform-wide audit log:
// a catch-all consumer writes every CloudEvent it receives (not just saga
// events) into this table with FlowID = uuid.Nil when the event is not
// part of a tracked saga. The new indexed columns (SourceService,
// ResourceID, OrganizationID, Domain) let the audit HTTP API answer
// "show me everything that happened to resource X" fast.
type EventAudit struct {
	ID             uuid.UUID `gorm:"type:uuid;primaryKey" json:"id"`
	FlowID         uuid.UUID `gorm:"type:uuid;index" json:"flow_id"`
	SagaID         string    `gorm:"size:128;index" json:"saga_id"`
	EventType      string    `gorm:"size:128;not null;index:idx_audit_event_type" json:"event_type"`
	Domain         string    `gorm:"size:64;index:idx_audit_domain" json:"domain,omitempty"`
	Source         string    `gorm:"size:64;index:idx_audit_source" json:"source"`
	SourceService  string    `gorm:"size:64;index:idx_audit_source_service" json:"source_service,omitempty"`
	ResourceType   string    `gorm:"size:64;index:idx_audit_resource_type" json:"resource_type,omitempty"`
	ResourceID     string    `gorm:"size:128;index:idx_audit_resource_id" json:"resource_id,omitempty"`
	OrganizationID string    `gorm:"size:64;index:idx_audit_org" json:"organization_id,omitempty"`
	ActorID        string    `gorm:"size:64;index:idx_audit_actor" json:"actor_id,omitempty"`
	OccurredAt     time.Time `gorm:"not null;index:idx_audit_occurred_at" json:"occurred_at"`
	Payload        string    `gorm:"type:jsonb" json:"payload"`
	CreatedAt      time.Time `json:"created_at"`
}

func (EventAudit) TableName() string { return "pulse_event_audit" }
