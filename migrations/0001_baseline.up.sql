-- 0001_baseline — pulse-service schema baseline.
--
-- D-178 golang-migrate standardization. Reproduces, byte-for-byte, the schema
-- that GORM AutoMigrate (Flow, FlowStep, EventAudit) + ApplyRLSObjects
-- (rls_objects.go) produced at boot, so a fresh deploy builds an identical schema
-- with zero manual steps. Authored from a real pg_dump of the actual
-- AutoMigrate + RLS output, not hand-transcribed guesses.
--
-- Layout mirrors what the dump showed:
--   (1) 3 tables (pulse_flows, pulse_flow_steps, pulse_event_audit) with exact
--       columns/types/NOT NULL and primary keys;
--   (2) indexes (both the GORM model indexes AND the RLS-script org indexes —
--       the two org indexes per table coexist in the live schema);
--   (3) the pulse_flow_steps -> pulse_flows foreign key GORM created;
--   (4) ENABLE + FORCE ROW LEVEL SECURITY + tenant_isolation policy on all 3
--       tables (every pulse table is tenant-scoped);
--   (5) SECURITY DEFINER org-resolver functions owned by pulse_service_system,
--       + the SELECT grants its resolvers need.
--
-- The pulse_flow_steps org-column denormalization/backfill + sentinel backfill +
-- SET NOT NULL in rls_objects.go are no-ops on a fresh DB; the resulting schema
-- (organization_id uuid NOT NULL on all three + the org indexes) is expressed
-- directly below. The preMigrateSQL varchar->uuid retype is likewise a no-op on a
-- fresh DB (GORM already creates organization_id as uuid from the model).
--
-- Per-role GRANTs + ALTER DEFAULT PRIVILEGES come from the DB provisioning
-- (infrastructure/docker/init-databases.sql), applied before this migration runs
-- — they are NOT part of this migration.

-- ---------------------------------------------------------------------------
-- (1) Tables
-- ---------------------------------------------------------------------------

CREATE TABLE public.pulse_flows (
    id uuid NOT NULL,
    saga_id character varying(128) NOT NULL,
    kind character varying(64) NOT NULL,
    state character varying(32) NOT NULL,
    trigger_event character varying(128) NOT NULL,
    organization_id uuid NOT NULL,
    user_id character varying(64),
    current_step character varying(128),
    services text,
    steps_complete bigint,
    steps_total bigint,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    duration_ms bigint,
    error text,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    CONSTRAINT pulse_flows_pkey PRIMARY KEY (id)
);

CREATE TABLE public.pulse_flow_steps (
    id uuid NOT NULL,
    flow_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    step_name character varying(128) NOT NULL,
    service character varying(64) NOT NULL,
    event_type character varying(128) NOT NULL,
    status character varying(32) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    duration_ms bigint,
    error text,
    payload jsonb,
    created_at timestamp with time zone,
    CONSTRAINT pulse_flow_steps_pkey PRIMARY KEY (id)
);

CREATE TABLE public.pulse_event_audit (
    id uuid NOT NULL,
    flow_id uuid,
    saga_id character varying(128),
    event_type character varying(128) NOT NULL,
    domain character varying(64),
    source character varying(64),
    source_service character varying(64),
    resource_type character varying(64),
    resource_id character varying(128),
    organization_id uuid NOT NULL,
    actor_id character varying(64),
    occurred_at timestamp with time zone NOT NULL,
    payload jsonb,
    created_at timestamp with time zone,
    CONSTRAINT pulse_event_audit_pkey PRIMARY KEY (id)
);

-- ---------------------------------------------------------------------------
-- (2) Indexes. Both the GORM model indexes (idx_pulse_*_organization_id, the
--     named idx_audit_* set) AND the rls_objects.go org indexes
--     (idx_pulse_*_org) exist in the live schema — both are reproduced.
-- ---------------------------------------------------------------------------

CREATE UNIQUE INDEX idx_pulse_flows_saga_id ON public.pulse_flows USING btree (saga_id);
CREATE INDEX idx_pulse_flows_kind ON public.pulse_flows USING btree (kind);
CREATE INDEX idx_pulse_flows_state ON public.pulse_flows USING btree (state);
CREATE INDEX idx_pulse_flows_organization_id ON public.pulse_flows USING btree (organization_id);
CREATE INDEX idx_pulse_flows_user_id ON public.pulse_flows USING btree (user_id);
CREATE INDEX idx_pulse_flows_org ON public.pulse_flows USING btree (organization_id);

CREATE INDEX idx_pulse_flow_steps_flow_id ON public.pulse_flow_steps USING btree (flow_id);
CREATE INDEX idx_pulse_flow_steps_organization_id ON public.pulse_flow_steps USING btree (organization_id);
CREATE INDEX idx_pulse_flow_steps_org ON public.pulse_flow_steps USING btree (organization_id);

CREATE INDEX idx_pulse_event_audit_flow_id ON public.pulse_event_audit USING btree (flow_id);
CREATE INDEX idx_pulse_event_audit_saga_id ON public.pulse_event_audit USING btree (saga_id);
CREATE INDEX idx_audit_event_type ON public.pulse_event_audit USING btree (event_type);
CREATE INDEX idx_audit_domain ON public.pulse_event_audit USING btree (domain);
CREATE INDEX idx_audit_source ON public.pulse_event_audit USING btree (source);
CREATE INDEX idx_audit_source_service ON public.pulse_event_audit USING btree (source_service);
CREATE INDEX idx_audit_resource_type ON public.pulse_event_audit USING btree (resource_type);
CREATE INDEX idx_audit_resource_id ON public.pulse_event_audit USING btree (resource_id);
CREATE INDEX idx_audit_org ON public.pulse_event_audit USING btree (organization_id);
CREATE INDEX idx_audit_actor ON public.pulse_event_audit USING btree (actor_id);
CREATE INDEX idx_audit_occurred_at ON public.pulse_event_audit USING btree (occurred_at);
CREATE INDEX idx_pulse_event_audit_org ON public.pulse_event_audit USING btree (organization_id);

-- ---------------------------------------------------------------------------
-- (3) Foreign key: pulse_flow_steps.flow_id -> pulse_flows.id (GORM created it
--     from Flow.Steps foreignKey:FlowID;references:ID).
-- ---------------------------------------------------------------------------

ALTER TABLE ONLY public.pulse_flow_steps
    ADD CONSTRAINT fk_pulse_flows_steps FOREIGN KEY (flow_id) REFERENCES public.pulse_flows(id);

-- ---------------------------------------------------------------------------
-- (4) Row-level security: ENABLE + FORCE + tenant_isolation on all 3 tables.
--     The predicate reads the tx-local app.current_org GUC stamped by
--     platform-kit/tenantdb.Stamp; unset -> NULL -> zero rows (fail-closed).
-- ---------------------------------------------------------------------------

ALTER TABLE public.pulse_flows ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_flows FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON public.pulse_flows
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

ALTER TABLE public.pulse_flow_steps ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_flow_steps FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON public.pulse_flow_steps
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

ALTER TABLE public.pulse_event_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pulse_event_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON public.pulse_event_audit
    USING (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid)
    WITH CHECK (organization_id = NULLIF(current_setting('app.current_org', true), '')::uuid);

-- ---------------------------------------------------------------------------
-- (5) SECURITY DEFINER org resolvers, owned by pulse_service_system (NOLOGIN,
--     BYPASSRLS) — the only BYPASSRLS surface, so no BYPASSRLS login credential
--     exists. Pinned search_path prevents search-path hijacking.
-- ---------------------------------------------------------------------------

-- rls_org_ids() — enumerates every tenant org for cross-tenant jobs/consumers
-- (D-071 Model D per-org loops) while the app connects NOBYPASSRLS. Union of
-- DISTINCT organization_id over pulse_flows and pulse_event_audit; the sentinel
-- org is excluded so cross-tenant jobs never iterate it.
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

-- rls_flow_org(p_id) — resolves the owning org of a flow by its PK.
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

-- rls_audit_org(p_id) — resolves the owning org of an audit row by its PK.
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

-- rls_saga_org(p_saga_id) — resolves the owning org of a flow by its saga_id,
-- for the FlowTracker consumer: a late-arriving saga step event that carries no
-- org in its payload resolves the org of the flow started under the same saga_id.
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

-- The SECURITY DEFINER functions run as pulse_service_system (BYPASSRLS) but hold
-- no table grants by default; grant SELECT on the anchors its resolvers read.
GRANT SELECT ON public.pulse_flows, public.pulse_event_audit TO pulse_service_system;
