package http

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/sentiae/platform-kit/middleware"
	"github.com/sentiae/platform-kit/tenant"
)

// OrgResolver resolves the owning org of a by-id resource via the D-072
// SECURITY DEFINER rls_* functions, so an id-only handler (no organization_id in
// the request) can scope RLS without trusting caller input. Satisfied by the
// postgres TenantResolverRepo; the interface keeps this handler package free of a
// repository import (hexagonal direction). DI passes the concrete impl.
type OrgResolver interface {
	ResolveFlowOrg(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
	ResolveAuditOrg(ctx context.Context, id uuid.UUID) (uuid.UUID, error)
}

// Org-scoping sentinel errors returned by the request helpers; respondOrgError
// maps each to the HTTP status the fleet contract requires (missing/invalid org
// → 400, authz failure → 403, cross-org by-id hit → 404 so existence never
// leaks).
var (
	errOrgRequired      = errors.New("organization_id is required")
	errOrgInvalid       = errors.New("invalid organization_id")
	errOrgForbidden     = errors.New("not authorized for organization")
	errResourceNotFound = errors.New("not found")
)

// authMiddleware authenticates every request under /api/v1/pulse and /audit
// before it reaches a handler (the surface was previously UNAUTHENTICATED). Two
// accepted credentials:
//
//   - x-api-key == the shared service token → a SERVICE principal (headless
//     inter-service caller), validated in constant time via
//     tenant.ServiceTokenValidator (same secret the gRPC auth chain uses).
//   - Authorization: Bearer <jwt> → validated via the JWKS TokenValidator, then
//     promoted to a USER principal. Both the platform-kit claims key AND the
//     tenant.Principal are set (D-073 "BOTH keys" gotcha): tenant.FromContext
//     resolves the user principal (whose org memberships are the sole RLS
//     authority) while middleware.GetClaims still resolves the subject/scopes for
//     any permission middleware downstream.
//
// Neither present/valid → 401. /health and /ready are mounted OUTSIDE the groups
// this wraps, so they stay open.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Service-token path (headless inter-service caller).
		if key := r.Header.Get("x-api-key"); key != "" {
			svcToken := tenant.ServiceTokenValidator{Expected: s.serviceAPIKey}
			if err := svcToken.Validate(r.Context(), key); err != nil {
				respondWithError(w, http.StatusUnauthorized, "invalid service token")
				return
			}
			ctx := tenant.ContextWithPrincipal(r.Context(), tenant.Principal{ServiceAuthed: true})
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// User-token path (Bearer JWT).
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || s.jwks == nil {
			respondWithError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			respondWithError(w, http.StatusUnauthorized, "invalid authorization header format")
			return
		}
		claims, err := s.jwks.Validate(r.Context(), parts[1])
		if err != nil {
			respondWithError(w, http.StatusUnauthorized, "invalid or expired token")
			return
		}
		// Set BOTH keys: the platform-kit claims key (middleware.GetClaims) and an
		// explicit tenant.Principal (tenant.FromContext) — see the doc comment.
		ctx := middleware.InjectClaimsForTest(r.Context(), claims)
		cc := claims
		ctx = tenant.ContextWithPrincipal(ctx, tenant.Principal{Claims: &cc})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// orgFromRequest reads the org from the organization_id or org_id query param,
// falling back to the X-Organization-ID header (the BFF sets it), authorizes the
// authenticated principal for it (D-073: a user's memberships are the sole
// authority; a service principal is governed by its grants), and returns the org
// plus a ctx stamped with the active org so tenantdb.Enforce scopes every
// RLS-forced read the handler issues.
func orgFromRequest(r *http.Request) (uuid.UUID, context.Context, error) {
	orgStr := r.URL.Query().Get("organization_id")
	if orgStr == "" {
		orgStr = r.URL.Query().Get("org_id")
	}
	if orgStr == "" {
		orgStr = r.Header.Get("X-Organization-ID")
	}
	if orgStr == "" {
		return uuid.Nil, nil, errOrgRequired
	}
	org, err := uuid.Parse(orgStr)
	if err != nil {
		return uuid.Nil, nil, errOrgInvalid
	}
	ctx, err := authorizeAndStampOrg(r.Context(), org)
	if err != nil {
		return uuid.Nil, nil, err
	}
	return org, ctx, nil
}

// authorizeAndStampOrg authorizes the principal on ctx for org (denies a
// non-member — claims present = sole authority) and returns ctx stamped with the
// active org. Authorization failure collapses to errOrgForbidden (→ 403).
func authorizeAndStampOrg(ctx context.Context, org uuid.UUID) (context.Context, error) {
	if err := tenant.AuthorizeOrg(ctx, org); err != nil {
		return nil, errOrgForbidden
	}
	return tenant.WithActiveOrg(ctx, org), nil
}

// stampFlowOrg resolves the org owning flowID and returns a ctx stamped with it,
// 404-safe: an unknown id (resolver returns uuid.Nil) or a caller who may not act
// in the owning org both yield errResourceNotFound so a cross-org id cannot probe
// existence. A resolver DB error is returned verbatim (→ 500).
func (s *Server) stampFlowOrg(ctx context.Context, flowID uuid.UUID) (context.Context, error) {
	org, err := s.orgResolver.ResolveFlowOrg(ctx, flowID)
	if err != nil {
		return nil, err
	}
	return stampResolvedOrg(ctx, org)
}

// stampAuditOrg is stampFlowOrg for an audit-event id.
func (s *Server) stampAuditOrg(ctx context.Context, auditID uuid.UUID) (context.Context, error) {
	org, err := s.orgResolver.ResolveAuditOrg(ctx, auditID)
	if err != nil {
		return nil, err
	}
	return stampResolvedOrg(ctx, org)
}

// stampResolvedOrg applies the shared not-found-safe assert for a by-id resource:
// a Nil org (no such row) or a caller who may not act in the owning org both map
// to errResourceNotFound; otherwise stamp WithActiveOrg.
func stampResolvedOrg(ctx context.Context, org uuid.UUID) (context.Context, error) {
	if org == uuid.Nil {
		return nil, errResourceNotFound
	}
	if err := tenant.AssertOrgOrNotFound(ctx, org, errResourceNotFound.Error()); err != nil {
		return nil, errResourceNotFound
	}
	return tenant.WithActiveOrg(ctx, org), nil
}

// respondWithError writes pulse's {"error":...} envelope at the given status.
func respondWithError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

// respondOrgError maps an org-scoping sentinel to its HTTP status, preserving
// pulse's {"error":...} envelope.
func respondOrgError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errOrgRequired), errors.Is(err, errOrgInvalid):
		respondWithError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errOrgForbidden):
		respondWithError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, errResourceNotFound):
		respondWithError(w, http.StatusNotFound, err.Error())
	default:
		respondWithError(w, http.StatusInternalServerError, "internal server error")
	}
}
