-- ============================================================
-- CSTGlobal — PostgreSQL Schema
-- Version: 001_initial_schema
-- ============================================================

-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "postgis";        -- geospatial queries
CREATE EXTENSION IF NOT EXISTS "pg_trgm";        -- fuzzy text search

-- ============================================================
-- ENUMERATIONS
-- ============================================================

CREATE TYPE project_stage AS ENUM (
    'Planning',
    'Tender',
    'Awarded',
    'Under Construction',
    'Completed',
    'Cancelled'
);

CREATE TYPE project_sector AS ENUM (
    'Transport',
    'Infrastructure',
    'Energy',
    'Commercial',
    'Water',
    'Sport & Leisure',
    'Mixed Use',
    'Residential',
    'Healthcare',
    'Education'
);

CREATE TYPE project_region AS ENUM (
    'Middle East',
    'Asia Pacific',
    'Europe',
    'Americas',
    'Africa',
    'Central Asia'
);

CREATE TYPE stakeholder_role AS ENUM (
    'Owner',
    'Architect',
    'Main Contractor',
    'Subcontractor',
    'Engineer',
    'Quantity Surveyor',
    'Project Manager',
    'Funder'
);

CREATE TYPE lead_status AS ENUM (
    'Discovery',
    'Qualifying',
    'Bidding',
    'Won',
    'Lost'
);

CREATE TYPE user_plan AS ENUM (
    'Free',
    'Pro',
    'Enterprise'
);

CREATE TYPE industry_mode AS ENUM (
    'Construction',
    'Creative',
    'Software'
);

-- ============================================================
-- USERS & AUTH
-- ============================================================

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password TEXT,                          -- NULL if OAuth only
    full_name       VARCHAR(255),
    company         VARCHAR(255),
    plan            user_plan NOT NULL DEFAULT 'Free',
    industry_mode   industry_mode NOT NULL DEFAULT 'Construction',
    avatar_url      TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    is_verified     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at   TIMESTAMPTZ
);

CREATE TABLE oauth_accounts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider        VARCHAR(50) NOT NULL,          -- 'google', 'microsoft', 'github'
    provider_id     VARCHAR(255) NOT NULL,
    access_token    TEXT,
    refresh_token   TEXT,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, provider_id)
);

CREATE TABLE sessions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT UNIQUE NOT NULL,
    ip_address      INET,
    user_agent      TEXT,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- PROJECTS (core intelligence data)
-- ============================================================

CREATE TABLE projects (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    external_id         VARCHAR(255) UNIQUE,       -- ID from source system
    title               VARCHAR(500) NOT NULL,
    description         TEXT,
    value_usd           BIGINT,                    -- in cents to avoid float issues
    value_currency      CHAR(3) DEFAULT 'USD',
    value_raw           VARCHAR(100),              -- original value string from source
    location_display    VARCHAR(255),              -- human-readable e.g. "Dubai, UAE"
    location_country    VARCHAR(100),
    location_city       VARCHAR(100),
    location_coords     GEOGRAPHY(POINT, 4326),    -- PostGIS point
    region              project_region,
    sector              project_sector,
    stage               project_stage NOT NULL DEFAULT 'Planning',
    timeline_start      DATE,
    timeline_end        DATE,
    timeline_display    VARCHAR(100),              -- e.g. "2025–2029"

    -- Milestone tracking (JSONB for flexibility)
    milestones          JSONB DEFAULT '[]',
    -- e.g. [{"label": "Feasibility", "completed": true}, ...]
    active_milestone    SMALLINT DEFAULT 0,

    -- Source tracking
    source_name         VARCHAR(255),              -- e.g. "TED EU", "Sam.gov"
    source_url          TEXT,
    tender_document_url TEXT,
    last_verified_at    TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Content hash for deduplication
    content_hash        CHAR(64),                  -- SHA-256 of key fields

    -- Soft delete
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Full-text search index
CREATE INDEX idx_projects_fts ON projects
    USING GIN (to_tsvector('english', title || ' ' || COALESCE(description, '') || ' ' || COALESCE(location_display, '')));

-- Trigram index for ILIKE search
CREATE INDEX idx_projects_title_trgm ON projects USING GIN (title gin_trgm_ops);

-- Common filter indexes
CREATE INDEX idx_projects_region   ON projects (region);
CREATE INDEX idx_projects_sector   ON projects (sector);
CREATE INDEX idx_projects_stage    ON projects (stage);
CREATE INDEX idx_projects_value    ON projects (value_usd);
CREATE INDEX idx_projects_verified ON projects (last_verified_at DESC);
CREATE INDEX idx_projects_geom     ON projects USING GIST (location_coords);

-- ============================================================
-- STAKEHOLDERS
-- ============================================================

CREATE TABLE organisations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(500) NOT NULL,
    country         VARCHAR(100),
    website         TEXT,
    logo_url        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_organisations_name ON organisations (name);

CREATE TABLE project_stakeholders (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    organisation_id UUID REFERENCES organisations(id),
    name_raw        VARCHAR(500),                  -- raw name if org not matched
    role            stakeholder_role NOT NULL,
    is_confirmed    BOOLEAN DEFAULT FALSE,
    source_url      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, organisation_id, role)
);

CREATE INDEX idx_stakeholders_project ON project_stakeholders (project_id);
CREATE INDEX idx_stakeholders_org     ON project_stakeholders (organisation_id);

-- ============================================================
-- AI SCORING
-- ============================================================

CREATE TABLE project_scores (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id          UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id             UUID REFERENCES users(id) ON DELETE SET NULL,  -- NULL = global score
    total_score         SMALLINT NOT NULL CHECK (total_score BETWEEN 0 AND 100),
    match_score         SMALLINT NOT NULL DEFAULT 0,  -- w1 · Match  (max 40)
    budget_score        SMALLINT NOT NULL DEFAULT 0,  -- w2 · Budget (max 35)
    timeline_score      SMALLINT NOT NULL DEFAULT 0,  -- w3 · Timeline (max 25)
    scoring_version     VARCHAR(20) DEFAULT '1.0',
    factors             JSONB DEFAULT '{}',           -- breakdown detail
    calculated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (project_id, user_id)
);

CREATE INDEX idx_scores_project ON project_scores (project_id);
CREATE INDEX idx_scores_user    ON project_scores (user_id);

-- ============================================================
-- USER LEADS / CRM
-- ============================================================

CREATE TABLE leads (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    status          lead_status NOT NULL DEFAULT 'Discovery',
    notes           TEXT,
    bid_value       BIGINT,                         -- user's estimated bid (cents)
    probability_pct SMALLINT CHECK (probability_pct BETWEEN 0 AND 100),
    assigned_to     UUID REFERENCES users(id),
    tags            TEXT[] DEFAULT '{}',
    position        INTEGER DEFAULT 0,              -- kanban card order
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, project_id)
);

CREATE INDEX idx_leads_user    ON leads (user_id);
CREATE INDEX idx_leads_project ON leads (project_id);
CREATE INDEX idx_leads_status  ON leads (user_id, status);

CREATE TABLE lead_reminders (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id         UUID NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title           VARCHAR(255) NOT NULL,
    remind_at       TIMESTAMPTZ NOT NULL,
    is_sent         BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_reminders_lead    ON lead_reminders (lead_id);
CREATE INDEX idx_reminders_due     ON lead_reminders (remind_at) WHERE is_sent = FALSE;

-- ============================================================
-- CLIENT PORTAL — Comments & Feedback
-- ============================================================

CREATE TABLE portal_comments (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    author_id       UUID REFERENCES users(id) ON DELETE SET NULL,
    author_name     VARCHAR(255),                   -- for external (non-user) commenters
    milestone_index SMALLINT,                       -- which milestone this comment relates to
    body            TEXT NOT NULL,
    is_internal     BOOLEAN DEFAULT FALSE,           -- internal note vs client-visible
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_comments_project ON portal_comments (project_id);

-- ============================================================
-- ETL AUDIT LOG
-- ============================================================

CREATE TABLE etl_runs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name     VARCHAR(255) NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(50),                    -- 'running', 'success', 'failed'
    records_fetched INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_skipped INTEGER DEFAULT 0,              -- duplicates
    error_message   TEXT,
    metadata        JSONB DEFAULT '{}'
);

CREATE TABLE etl_dedup_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id          UUID REFERENCES etl_runs(id),
    content_hash    CHAR(64) NOT NULL,
    action          VARCHAR(20),                    -- 'insert', 'update', 'skip'
    project_id      UUID REFERENCES projects(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- HELPER: auto-update updated_at
-- ============================================================

CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at_users     BEFORE UPDATE ON users     FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
CREATE TRIGGER set_updated_at_projects  BEFORE UPDATE ON projects  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
CREATE TRIGGER set_updated_at_leads     BEFORE UPDATE ON leads     FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
CREATE TRIGGER set_updated_at_comments  BEFORE UPDATE ON portal_comments FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();

-- ============================================================
-- SEED: example data matching the UI mock
-- ============================================================

INSERT INTO organisations (id, name, country) VALUES
    ('a1000000-0000-0000-0000-000000000001', 'RTA Dubai', 'AE'),
    ('a1000000-0000-0000-0000-000000000002', 'Atkins Global', 'GB'),
    ('a1000000-0000-0000-0000-000000000003', 'Transport NSW', 'AU'),
    ('a1000000-0000-0000-0000-000000000004', 'WSP', 'CA'),
    ('a1000000-0000-0000-0000-000000000005', 'KPLC', 'KE'),
    ('a1000000-0000-0000-0000-000000000006', 'China Power Engineering', 'CN'),
    ('a1000000-0000-0000-0000-000000000007', 'Allianz Real Estate', 'DE'),
    ('a1000000-0000-0000-0000-000000000008', 'Snøhetta', 'NO'),
    ('a1000000-0000-0000-0000-000000000009', 'SABESP', 'BR'),
    ('a1000000-0000-0000-0000-000000000010', 'ROSHN', 'SA'),
    ('a1000000-0000-0000-0000-000000000011', 'Populous', 'US'),
    ('a1000000-0000-0000-0000-000000000012', 'Saudi Binladin Group', 'SA'),
    ('a1000000-0000-0000-0000-000000000013', 'LTA Singapore', 'SG'),
    ('a1000000-0000-0000-0000-000000000014', 'AECOM', 'US'),
    ('a1000000-0000-0000-0000-000000000015', 'LASG', 'NG'),
    ('a1000000-0000-0000-0000-000000000016', 'Kéré Architecture', 'DE');

INSERT INTO projects (
    id, title, description, value_usd, location_display, location_country, location_city,
    location_coords, region, sector, stage, timeline_display,
    milestones, active_milestone, source_name, last_verified_at
) VALUES
(
    'b1000000-0000-0000-0000-000000000001',
    'Dubai Metro Phase 4 Extension',
    '34km elevated metro extension with 20 new stations connecting outer suburbs to the existing Red and Green lines.',
    420000000000,
    'Dubai, UAE', 'AE', 'Dubai',
    ST_SetSRID(ST_MakePoint(55.2708, 25.2048), 4326),
    'Middle East', 'Transport', 'Tender', '2025–2029',
    '[{"label":"Feasibility Complete","completed":true},{"label":"EIA Approved","completed":true},{"label":"Tender Open","completed":false},{"label":"Award Pending","completed":false}]',
    2, 'TED EU', NOW() - INTERVAL '5 days'
),
(
    'b1000000-0000-0000-0000-000000000002',
    'Nairobi Green Energy Hub',
    'Mixed solar–wind generation complex with 400MW capacity and grid-scale battery storage for Nairobi metro area.',
    89000000000,
    'Nairobi, Kenya', 'KE', 'Nairobi',
    ST_SetSRID(ST_MakePoint(36.8219, -1.2921), 4326),
    'Africa', 'Energy', 'Awarded', '2025–2027',
    '[{"label":"Award Signed","completed":true},{"label":"Site Preparation","completed":true},{"label":"Construction","completed":false},{"label":"Commissioning","completed":false}]',
    1, 'KPLC Procurement Portal', NOW() - INTERVAL '1 day'
),
(
    'b1000000-0000-0000-0000-000000000003',
    'Riyadh Sports Boulevard',
    '12km linear sports district with 8 stadiums, cycling tracks, retail and hospitality as part of Vision 2030.',
    145000000000,
    'Riyadh, Saudi Arabia', 'SA', 'Riyadh',
    ST_SetSRID(ST_MakePoint(46.6753, 24.7136), 4326),
    'Middle East', 'Sport & Leisure', 'Awarded', '2024–2026',
    '[{"label":"Award Signed","completed":true},{"label":"Site Preparation","completed":true},{"label":"Structure","completed":true},{"label":"Fit-Out","completed":false}]',
    2, 'ROSHN Official', NOW() - INTERVAL '2 days'
);
