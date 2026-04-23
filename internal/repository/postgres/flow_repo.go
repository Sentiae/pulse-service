package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/sentiae/pulse-service/internal/domain"
)

// FlowRepository is the only data-access object pulse-service needs. It
// groups Flow, FlowStep, and EventAudit because they're always accessed
// together.
type FlowRepository struct{ db *gorm.DB }

func NewFlowRepository(db *gorm.DB) *FlowRepository { return &FlowRepository{db: db} }

// CreateFlow inserts a new flow row.
func (r *FlowRepository) CreateFlow(ctx context.Context, flow *domain.Flow) error {
	if flow.ID == uuid.Nil {
		flow.ID = uuid.New()
	}
	return r.db.WithContext(ctx).Create(flow).Error
}

// GetFlow fetches a flow by id with its steps preloaded (ordered by
// StartedAt ascending so the UI gets a deterministic sequence).
func (r *FlowRepository) GetFlow(ctx context.Context, id uuid.UUID) (*domain.Flow, error) {
	var flow domain.Flow
	err := r.db.WithContext(ctx).
		Preload("Steps", func(tx *gorm.DB) *gorm.DB {
			return tx.Order("started_at asc")
		}).
		First(&flow, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrFlowNotFound
	}
	if err != nil {
		return nil, err
	}
	return &flow, nil
}

// GetFlowBySagaID is used by the consumer to look up an existing flow when
// it sees a step event.
func (r *FlowRepository) GetFlowBySagaID(ctx context.Context, sagaID string) (*domain.Flow, error) {
	var flow domain.Flow
	err := r.db.WithContext(ctx).
		Preload("Steps").
		First(&flow, "saga_id = ?", sagaID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrFlowNotFound
	}
	if err != nil {
		return nil, err
	}
	return &flow, nil
}

// UpdateFlow overwrites a flow's mutable fields. Steps are appended via
// AppendStep; this method doesn't touch them.
func (r *FlowRepository) UpdateFlow(ctx context.Context, flow *domain.Flow) error {
	return r.db.WithContext(ctx).
		Model(&domain.Flow{}).
		Where("id = ?", flow.ID).
		Updates(map[string]any{
			"state":          flow.State,
			"current_step":   flow.CurrentStep,
			"services":       flow.Services,
			"steps_complete": flow.StepsComplete,
			"steps_total":    flow.StepsTotal,
			"completed_at":   flow.CompletedAt,
			"duration_ms":    flow.DurationMS,
			"error":          flow.Error,
			"updated_at":     time.Now().UTC(),
		}).Error
}

// AppendStep creates a new step under the flow.
func (r *FlowRepository) AppendStep(ctx context.Context, step *domain.FlowStep) error {
	if step.ID == uuid.Nil {
		step.ID = uuid.New()
	}
	return r.db.WithContext(ctx).Create(step).Error
}

// AppendEventAudit persists the raw event for replay.
func (r *FlowRepository) AppendEventAudit(ctx context.Context, ev *domain.EventAudit) error {
	if ev.ID == uuid.Nil {
		ev.ID = uuid.New()
	}
	return r.db.WithContext(ctx).Create(ev).Error
}

// ListAuditForFlow returns the full event sequence for a flow, ordered.
func (r *FlowRepository) ListAuditForFlow(ctx context.Context, flowID uuid.UUID) ([]domain.EventAudit, error) {
	var events []domain.EventAudit
	err := r.db.WithContext(ctx).
		Where("flow_id = ?", flowID).
		Order("occurred_at asc").
		Find(&events).Error
	return events, err
}

// AuditFilter constrains ListAudit. Any zero-valued field is skipped.
type AuditFilter struct {
	EventType      string
	Domain         string
	SourceService  string
	ResourceID     string
	OrganizationID string
	ActorID        string
	From           *time.Time
	To             *time.Time
	Limit          int
	Offset         int
}

// ListAudit returns audit rows matching the filter, most recent first.
// Bounded by Limit (default 100, max 500).
func (r *FlowRepository) ListAudit(ctx context.Context, f AuditFilter) ([]domain.EventAudit, error) {
	q := r.db.WithContext(ctx).Model(&domain.EventAudit{}).Order("occurred_at desc")
	if f.EventType != "" {
		q = q.Where("event_type = ?", f.EventType)
	}
	if f.Domain != "" {
		q = q.Where("domain = ?", f.Domain)
	}
	if f.SourceService != "" {
		q = q.Where("source_service = ?", f.SourceService)
	}
	if f.ResourceID != "" {
		q = q.Where("resource_id = ?", f.ResourceID)
	}
	if f.OrganizationID != "" {
		q = q.Where("organization_id = ?", f.OrganizationID)
	}
	if f.ActorID != "" {
		q = q.Where("actor_id = ?", f.ActorID)
	}
	if f.From != nil {
		q = q.Where("occurred_at >= ?", *f.From)
	}
	if f.To != nil {
		q = q.Where("occurred_at <= ?", *f.To)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	q = q.Limit(limit)
	if f.Offset > 0 {
		q = q.Offset(f.Offset)
	}
	var rows []domain.EventAudit
	if err := q.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// GetAuditByID returns a single audit row.
func (r *FlowRepository) GetAuditByID(ctx context.Context, id uuid.UUID) (*domain.EventAudit, error) {
	var row domain.EventAudit
	err := r.db.WithContext(ctx).First(&row, "id = ?", id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, domain.ErrAuditNotFound
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// AppendAudit writes a generic audit row (not tied to a flow).
func (r *FlowRepository) AppendAudit(ctx context.Context, ev *domain.EventAudit) error {
	if ev.ID == uuid.Nil {
		ev.ID = uuid.New()
	}
	return r.db.WithContext(ctx).Create(ev).Error
}

// ListFlowsFilter holds optional filters for ListFlows.
type ListFlowsFilter struct {
	State FilterState
	Kind  domain.FlowKind
	Limit int
}

// FilterState lets callers pass "active" / "completed" / "failed" / "" (all).
type FilterState string

const (
	FilterActive    FilterState = "active"
	FilterCompleted FilterState = "completed"
	FilterFailed    FilterState = "failed"
	FilterAll       FilterState = ""
)

// ListFlows returns flows matching the given filter, most recent first.
func (r *FlowRepository) ListFlows(ctx context.Context, f ListFlowsFilter) ([]domain.Flow, error) {
	q := r.db.WithContext(ctx).Model(&domain.Flow{}).
		Order("started_at desc")

	switch f.State {
	case FilterActive:
		q = q.Where("state = ?", domain.FlowStateRunning)
	case FilterCompleted:
		q = q.Where("state = ?", domain.FlowStateCompleted)
	case FilterFailed:
		q = q.Where("state = ?", domain.FlowStateFailed)
	}
	if f.Kind != "" {
		q = q.Where("kind = ?", f.Kind)
	}
	limit := f.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	q = q.Limit(limit)

	var flows []domain.Flow
	if err := q.Find(&flows).Error; err != nil {
		return nil, err
	}
	return flows, nil
}

// Stats returns a summary of current flow activity.
type Stats struct {
	TotalActive    int64            `json:"total_active"`
	TotalCompleted int64            `json:"total_completed"`
	TotalFailed    int64            `json:"total_failed"`
	ActiveByKind   map[string]int64 `json:"active_by_kind"`
	AvgDurationMS  int64            `json:"avg_duration_ms"`
}

func (r *FlowRepository) Stats(ctx context.Context) (*Stats, error) {
	out := &Stats{ActiveByKind: map[string]int64{}}

	counts := []struct {
		State domain.FlowState
		N     int64
	}{}
	if err := r.db.WithContext(ctx).
		Model(&domain.Flow{}).
		Select("state, count(*) as n").
		Group("state").
		Scan(&counts).Error; err != nil {
		return nil, err
	}
	for _, c := range counts {
		switch c.State {
		case domain.FlowStateRunning:
			out.TotalActive = c.N
		case domain.FlowStateCompleted:
			out.TotalCompleted = c.N
		case domain.FlowStateFailed:
			out.TotalFailed = c.N
		}
	}

	byKind := []struct {
		Kind string
		N    int64
	}{}
	if err := r.db.WithContext(ctx).
		Model(&domain.Flow{}).
		Select("kind, count(*) as n").
		Where("state = ?", domain.FlowStateRunning).
		Group("kind").
		Scan(&byKind).Error; err != nil {
		return nil, err
	}
	for _, c := range byKind {
		out.ActiveByKind[c.Kind] = c.N
	}

	row := struct{ Avg float64 }{}
	if err := r.db.WithContext(ctx).
		Model(&domain.Flow{}).
		Select("coalesce(avg(duration_ms), 0) as avg").
		Where("state = ? AND duration_ms > 0", domain.FlowStateCompleted).
		Scan(&row).Error; err != nil {
		return nil, err
	}
	out.AvgDurationMS = int64(row.Avg)

	return out, nil
}
