-- 0001_baseline (down) — drop everything 0001_baseline.up.sql created, in reverse
-- dependency order: the SECURITY DEFINER resolver functions first (SQL functions
-- carry hard dependencies on the tables they read), then the tables (their
-- policies, RLS state, indexes, the foreign key, and PK constraints drop with
-- them). pulse_flow_steps before pulse_flows (FK dependency).

DROP FUNCTION IF EXISTS public.rls_saga_org(text);
DROP FUNCTION IF EXISTS public.rls_audit_org(uuid);
DROP FUNCTION IF EXISTS public.rls_flow_org(uuid);
DROP FUNCTION IF EXISTS public.rls_org_ids();

DROP TABLE IF EXISTS public.pulse_event_audit;
DROP TABLE IF EXISTS public.pulse_flow_steps;
DROP TABLE IF EXISTS public.pulse_flows;
