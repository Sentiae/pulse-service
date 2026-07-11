package postgres

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// P4 Postgres RLS (D-070/071/072) — pulse-service.
//
// pulse-service builds its schema with GORM AutoMigrate at boot (no
// golang-migrate). So — like notification-service and augur — the RLS objects
// live here as Go string constants applied idempotently under the OWNER
// connection. A clean deploy reproduces RLS with zero manual steps.
//
// Each statement is Exec'd one at a time: GORM's db.Exec runs through pgx's
// EXTENDED protocol (a prepared statement), which rejects multiple commands in
// one call (SQLSTATE 42601). splitSQLStatements is dollar-quote aware so a ';'
// inside a function body or a DO block does not split the statement.
//
// Must be called with the OWNER connection (pulse_service_owner): it retypes
// columns, alters table RLS state, and transfers function ownership to
// pulse_service_system, which the NOBYPASSRLS app role cannot do.
//
// 3 tenant tables get tenant_isolation RLS: pulse_flows, pulse_flow_steps,
// pulse_event_audit. All three carry (or gain) an organization_id uuid column.

// sentinelOrg is the platform sentinel organization id. Org-less platform events
// (audit rows / flows with no org in the CloudEvent) are stamped to it; the org
// enumerator excludes it so cross-tenant jobs never iterate the sentinel.
const sentinelOrg = "00000000-0000-0000-0000-000000000001"

// preMigrateSQL retypes the legacy varchar(64) organization_id columns on
// pulse_flows and pulse_event_audit to uuid. Guarded by information_schema so it
// is idempotent and a no-op once the column is already uuid. MUST run BEFORE
// AutoMigrate: GORM AutoMigrate cannot retype varchar->uuid, and the GORM models
// now map organization_id as uuid, so a stale varchar column would otherwise be
// left untouched (or conflict). Non-uuid dev rows are dropped first (D-046
// delete-now-rebuild-later; the live tables are empty so there is nothing to
// lose) so the ALTER ... USING cast succeeds.
const preMigrateSQL = `
DO $retype_flows$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'pulse_flows'
               AND column_name = 'organization_id' AND data_type = 'character varying') THEN
    DELETE FROM public.pulse_flows
     WHERE organization_id IS NOT NULL AND organization_id <> ''
       AND organization_id !~* '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$';
    ALTER TABLE public.pulse_flows
      ALTER COLUMN organization_id TYPE uuid USING NULLIF(organization_id, '')::uuid;
  END IF;
END
$retype_flows$;

DO $retype_audit$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = 'pulse_event_audit'
               AND column_name = 'organization_id' AND data_type = 'character varying') THEN
    DELETE FROM public.pulse_event_audit
     WHERE organization_id IS NOT NULL AND organization_id <> ''
       AND organization_id !~* '^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$';
    ALTER TABLE public.pulse_event_audit
      ALTER COLUMN organization_id TYPE uuid USING NULLIF(organization_id, '')::uuid;
  END IF;
END
$retype_audit$;
`

// rlsObjectsSQL is the full idempotent RLS object script, applied under the owner
// role after AutoMigrate. Order: (a) add organization_id to pulse_flow_steps and
// backfill it from the parent flow, delete orphans; (b) backfill NULL org on all
// three tables to the sentinel + SET NOT NULL + org indexes; (c) ENABLE+FORCE RLS
// + tenant_isolation policy on all three; (d) SECURITY DEFINER org-resolver
// functions owned by pulse_service_system; (e) grant _system SELECT on the anchors.
const rlsObjectsSQL = `
-- (a) pulse_flow_steps carries no organization_id of its own. Add it (no DEFAULT —
-- it is denormalized from the parent flow, then made NOT NULL), backfill from the
-- parent, then delete orphaned steps (delete-now-rebuild-later, D-046).
ALTER TABLE public.pulse_flow_steps ADD COLUMN IF NOT EXISTS organization_id uuid;
UPDATE public.pulse_flow_steps s
   SET organization_id = f.organization_id
  FROM public.pulse_flows f
 WHERE s.flow_id = f.id
   AND s.organization_id IS NULL;
DELETE FROM public.pulse_flow_steps WHERE organization_id IS NULL;

-- (b) backfill any remaining NULL org on the parent tables to the platform
-- sentinel org so SET NOT NULL succeeds, then enforce presence + index it for the
-- RLS predicate.
UPDATE public.pulse_flows       SET organization_id = '` + sentinelOrg + `' WHERE organization_id IS NULL;
UPDATE public.pulse_event_audit SET organization_id = '` + sentinelOrg + `' WHERE organization_id IS NULL;
ALTER TABLE public.pulse_flows       ALTER COLUMN organization_id SET NOT NULL;
ALTER TABLE public.pulse_flow_steps  ALTER COLUMN organization_id SET NOT NULL;
ALTER TABLE public.pulse_event_audit ALTER COLUMN organization_id SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pulse_flows_org       ON public.pulse_flows(organization_id);
CREATE INDEX IF NOT EXISTS idx_pulse_flow_steps_org  ON public.pulse_flow_steps(organization_id);
CREATE INDEX IF NOT EXISTS idx_pulse_event_audit_org ON public.pulse_event_audit(organization_id);

-- (c) tenant isolation: ENABLE + FORCE RLS with USING (reads/updates/deletes see
-- only the current org) and WITH CHECK (writes belong to the current org). The
-- predicate reads the tx-local app.current_org GUC stamped by
-- platform-kit/tenantdb.Stamp; unset -> NULL -> zero rows (fail-closed).
ALTER TABLE public.pulse_flows ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_flows FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON public.pulse_flows;
CREATE POLICY tenant_isolation ON public.pulse_flows
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

ALTER TABLE public.pulse_flow_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_flow_steps FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON public.pulse_flow_steps;
CREATE POLICY tenant_isolation ON public.pulse_flow_steps
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

ALTER TABLE public.pulse_event_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_event_audit FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON public.pulse_event_audit;
CREATE POLICY tenant_isolation ON public.pulse_event_audit
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

-- (d) SECURITY DEFINER org resolvers owned by pulse_service_system (NOLOGIN
-- BYPASSRLS): the ONLY BYPASSRLS surface, so no BYPASSRLS login credential
-- exists. A pinned search_path prevents search-path hijacking.
--
-- rls_org_ids() enumerates every tenant org for cross-tenant jobs/consumers
-- (D-071 Model D per-org loops) while the app connects NOBYPASSRLS. Union of
-- DISTINCT organization_id over pulse_flows and pulse_event_audit; the sentinel
-- org is excluded.
CREATE OR REPLACE FUNCTION public.rls_org_ids()
    RETURNS SETOF uuid
    LANGUAGE sql
    SECURITY DEFINER
    SET search_path = public, pg_temp
    STABLE
AS $rls_org_ids$
    SELECT DISTINCT organization_id
    FROM (
        SELECT organization_id FROM public.pulse_flows
        UNION SELECT organization_id FROM public.pulse_event_audit
    ) o
    WHERE organization_id IS NOT NULL
      AND organization_id <> '00000000-0000-0000-0000-000000000001'::uuid
$rls_org_ids$;
ALTER FUNCTION public.rls_org_ids() OWNER TO pulse_service_system;
REVOKE EXECUTE ON FUNCTION public.rls_org_ids() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.rls_org_ids() TO pulse_service_app;

-- rls_flow_org(p_id) resolves the owning org of a flow by its PK, for by-id
-- reads/mutations that arrive without an org in scope (D-072).
CREATE OR REPLACE FUNCTION public.rls_flow_org(p_id uuid)
    RETURNS uuid
    LANGUAGE sql
    SECURITY DEFINER
    SET search_path = public, pg_temp
    STABLE
AS $rls_flow_org$
    SELECT organization_id FROM public.pulse_flows WHERE id = p_id
$rls_flow_org$;
ALTER FUNCTION public.rls_flow_org(uuid) OWNER TO pulse_service_system;
REVOKE EXECUTE ON FUNCTION public.rls_flow_org(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.rls_flow_org(uuid) TO pulse_service_app;

-- rls_audit_org(p_id) resolves the owning org of an audit row by its PK.
CREATE OR REPLACE FUNCTION public.rls_audit_org(p_id uuid)
    RETURNS uuid
    LANGUAGE sql
    SECURITY DEFINER
    SET search_path = public, pg_temp
    STABLE
AS $rls_audit_org$
    SELECT organization_id FROM public.pulse_event_audit WHERE id = p_id
$rls_audit_org$;
ALTER FUNCTION public.rls_audit_org(uuid) OWNER TO pulse_service_system;
REVOKE EXECUTE ON FUNCTION public.rls_audit_org(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.rls_audit_org(uuid) TO pulse_service_app;

-- rls_saga_org(p_saga_id) resolves the owning org of a flow by its saga_id, for
-- the FlowTracker consumer: a late-arriving saga step event that carries no org
-- in its payload resolves the org of the flow started under the same saga_id.
CREATE OR REPLACE FUNCTION public.rls_saga_org(p_saga_id text)
    RETURNS uuid
    LANGUAGE sql
    SECURITY DEFINER
    SET search_path = public, pg_temp
    STABLE
AS $rls_saga_org$
    SELECT organization_id FROM public.pulse_flows WHERE saga_id = p_saga_id
$rls_saga_org$;
ALTER FUNCTION public.rls_saga_org(text) OWNER TO pulse_service_system;
REVOKE EXECUTE ON FUNCTION public.rls_saga_org(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.rls_saga_org(text) TO pulse_service_app;

-- (e) the SECURITY DEFINER functions run as pulse_service_system, which is
-- BYPASSRLS but has no table grants by default; grant it SELECT on the anchors
-- its resolvers read — minimal surface (SELECT only).
GRANT SELECT ON public.pulse_flows, public.pulse_event_audit TO pulse_service_system;
`

// ApplyPreMigrate retypes the legacy varchar(64) organization_id columns on
// pulse_flows and pulse_event_audit to uuid. Idempotent (no-op once already uuid)
// and UNCONDITIONAL: the caller runs it under the OWNER connection BEFORE
// AutoMigrate, not gated on the RLS flag, because the GORM models map
// organization_id as uuid and AutoMigrate cannot perform the retype.
func ApplyPreMigrate(ownerDB *gorm.DB) error {
	for j, stmt := range splitSQLStatements(preMigrateSQL) {
		if err := ownerDB.Exec(stmt).Error; err != nil {
			return fmt.Errorf("apply pre-migrate stmt #%d: %w", j, err)
		}
	}
	return nil
}

// ApplyRLSObjects (re)applies the RLS objects — pulse_flow_steps org-column
// denormalization + backfill + NOT NULL, tenant_isolation policies on all three
// tables, and the SECURITY DEFINER org resolvers — on top of the AutoMigrate
// schema. Idempotent; MUST be called under the OWNER connection AFTER AutoMigrate.
func ApplyRLSObjects(ownerDB *gorm.DB) error {
	for j, stmt := range splitSQLStatements(rlsObjectsSQL) {
		if err := ownerDB.Exec(stmt).Error; err != nil {
			return fmt.Errorf("apply RLS object stmt #%d: %w", j, err)
		}
	}
	return nil
}

// splitSQLStatements splits a SQL script into individual statements on top-level
// ';', tracking `$tag$ ... $tag$` dollar-quoted blocks (so a ';' inside a
// function body or a DO block is not a boundary) and skipping '--' line comments.
// Adequate for the RLS DDL scripts (no single-quoted strings containing ';', no
// /* */ comments); it is NOT a general-purpose SQL parser.
func splitSQLStatements(script string) []string {
	var out []string
	var cur strings.Builder
	var dollarTag string // non-empty while inside a $tag$...$tag$ block

	lines := strings.Split(script, "\n")
	for _, line := range lines {
		// Strip a full-line or trailing '--' comment only when not inside a
		// dollar-quoted block.
		if dollarTag == "" {
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
		}
		cur.WriteString(line)
		cur.WriteString("\n")

		i := 0
		for i < len(line) {
			if dollarTag == "" {
				if line[i] == '$' {
					if tag, n := readDollarTag(line, i); n > 0 {
						dollarTag = tag
						i += n
						continue
					}
				}
				if line[i] == ';' {
					stmt := strings.TrimSpace(cur.String())
					if stmt != "" && stmt != ";" {
						out = append(out, strings.TrimSuffix(stmt, ";")+";")
					}
					cur.Reset()
				}
				i++
			} else {
				if line[i] == '$' {
					if tag, n := readDollarTag(line, i); n > 0 && tag == dollarTag {
						dollarTag = ""
						i += n
						continue
					}
				}
				i++
			}
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}
	return out
}

// readDollarTag reads a dollar-quote delimiter starting at s[i] (which is '$').
// Returns the full delimiter (e.g. "$$" or "$fn$") and its length, or ("", 0) if
// s[i] does not begin a valid dollar tag.
func readDollarTag(s string, i int) (string, int) {
	if i >= len(s) || s[i] != '$' {
		return "", 0
	}
	j := i + 1
	for j < len(s) && s[j] != '$' {
		c := s[j]
		if c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
			return "", 0 // not a valid tag body
		}
		j++
	}
	if j >= len(s) {
		return "", 0 // no closing '$'
	}
	return s[i : j+1], j + 1 - i
}
