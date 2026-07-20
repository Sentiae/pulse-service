package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
	"gorm.io/gorm"

	"github.com/sentiae/platform-kit/tenant"
)

// TenantResolverRepo resolves owning orgs via the D-072 SECURITY DEFINER rls_*
// functions (migrations/0001_baseline.up.sql). Each lookup runs under tenant.WithSystemContext so
// the Enforce plugin skips stamping this bootstrap query — the function itself
// resolves the org with definer privileges while the app pool stays NOBYPASSRLS.
type TenantResolverRepo struct {
	db *gorm.DB
}

// NewTenantResolverRepo builds the resolver on the app pool (the rls_* functions
// are EXECUTE-granted to pulse_service_app).
func NewTenantResolverRepo(db *gorm.DB) *TenantResolverRepo {
	return &TenantResolverRepo{db: db}
}

// ListOrgIDs enumerates every tenant org for a cross-org sweep — it runs the
// SECURITY DEFINER rls_org_ids() fn under tenant.WithSystemContext so the Enforce
// plugin skips stamping this bootstrap query; the fn resolves the orgs with
// definer privileges while the app pool itself stays NOBYPASSRLS.
func (r *TenantResolverRepo) ListOrgIDs(ctx context.Context) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	sysCtx := tenant.WithSystemContext(ctx)
	if err := r.db.WithContext(sysCtx).Raw(`SELECT * FROM rls_org_ids()`).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("list org ids: %w", err)
	}
	return ids, nil
}

// ResolveFlowOrg returns the org owning the flow with the given id, or uuid.Nil
// (no error) when the fn returns NULL (no such row — the miss path). The result
// is scanned into sql.NullString then parsed: scanning a NULL uuid directly into
// *uuid.UUID panics (the fleet gotcha, D-072).
func (r *TenantResolverRepo) ResolveFlowOrg(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var raw sql.NullString
	sysCtx := tenant.WithSystemContext(ctx)
	if err := r.db.WithContext(sysCtx).Raw(`SELECT public.rls_flow_org(?)`, id).Scan(&raw).Error; err != nil {
		return uuid.Nil, fmt.Errorf("resolve flow org: %w", err)
	}
	if !raw.Valid || raw.String == "" {
		return uuid.Nil, nil
	}
	org, err := uuid.Parse(raw.String)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse resolved flow org: %w", err)
	}
	return org, nil
}

// ResolveAuditOrg returns the org owning the audit row with the given id, or
// uuid.Nil (no error) on a NULL result (the miss path).
func (r *TenantResolverRepo) ResolveAuditOrg(ctx context.Context, id uuid.UUID) (uuid.UUID, error) {
	var raw sql.NullString
	sysCtx := tenant.WithSystemContext(ctx)
	if err := r.db.WithContext(sysCtx).Raw(`SELECT public.rls_audit_org(?)`, id).Scan(&raw).Error; err != nil {
		return uuid.Nil, fmt.Errorf("resolve audit org: %w", err)
	}
	if !raw.Valid || raw.String == "" {
		return uuid.Nil, nil
	}
	org, err := uuid.Parse(raw.String)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse resolved audit org: %w", err)
	}
	return org, nil
}

// ResolveSagaOrg returns the org owning the flow with the given saga_id, or
// uuid.Nil (no error) on a NULL result (no flow yet for this saga — the miss
// path). Used by FlowTracker to resolve the org of a late-arriving saga step
// event that carries no org in its own payload.
func (r *TenantResolverRepo) ResolveSagaOrg(ctx context.Context, sagaID string) (uuid.UUID, error) {
	var raw sql.NullString
	sysCtx := tenant.WithSystemContext(ctx)
	if err := r.db.WithContext(sysCtx).Raw(`SELECT public.rls_saga_org(?)`, sagaID).Scan(&raw).Error; err != nil {
		return uuid.Nil, fmt.Errorf("resolve saga org: %w", err)
	}
	if !raw.Valid || raw.String == "" {
		return uuid.Nil, nil
	}
	org, err := uuid.Parse(raw.String)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse resolved saga org: %w", err)
	}
	return org, nil
}
