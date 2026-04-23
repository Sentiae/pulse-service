package domain

import "time"

// AlertActivityEntry is a single firing-alert activity item surfaced
// through the Pulse live stream. It is an in-memory projection (not
// persisted) so the live subscription stays cheap; the authoritative
// alert record lives in ops-service.
type AlertActivityEntry struct {
	ID          string    `json:"id"`
	Severity    string    `json:"severity"`
	Summary     string    `json:"summary"`
	ServiceID   string    `json:"service_id,omitempty"`
	ServiceName string    `json:"service_name,omitempty"`
	OrgID       string    `json:"org_id,omitempty"`
	Status      string    `json:"status"`
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// DeployActivityEntry is a single in-flight / recently-completed deploy
// activity item surfaced through the Pulse live stream. In-memory only:
// completed entries are retained for a short retention window so the
// landing page can show "just shipped" deploys for a few minutes.
type DeployActivityEntry struct {
	ID              string    `json:"id"`
	ServiceID       string    `json:"service_id,omitempty"`
	ServiceName     string    `json:"service_name,omitempty"`
	Environment     string    `json:"environment,omitempty"`
	Status          string    `json:"status"`
	Strategy        string    `json:"strategy,omitempty"`
	ProgressPct     int       `json:"progress_pct"`
	OrgID           string    `json:"org_id,omitempty"`
	StartedAt       time.Time `json:"started_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	DurationSeconds int64     `json:"duration_seconds"`
}
