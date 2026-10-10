-- ECOBrio Production Relational Schema
-- Target: PostgreSQL 16
-- Enforces: Concurrency constraints, Double-Entry Ledger, Outbox, and Idempotency Invariants

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Waste Streams Enum
DO $$ BEGIN
    CREATE TYPE waste_stream_type AS ENUM (
        'ORGANIC',
        'PLASTICS',
        'GLASS',
        'METALS',
        'PAPER',
        'EWASTE',
        'HAZARDOUS'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- 1. Users Table (Citizens, Collectors, Municipal Admins)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    role VARCHAR(32) NOT NULL CHECK (role IN ('CITIZEN', 'COLLECTOR', 'ADMIN')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Collection Hubs (Physical Depots & Smart Drop-off Terminals)
CREATE TABLE IF NOT EXISTS collection_hubs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(128) NOT NULL,
    address TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'MAINTENANCE', 'OFFLINE')),
    operating_hours VARCHAR(64) NOT NULL DEFAULT '06:00 - 22:00 Daily',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Collection Slots (Capacity Reservation with Capacity Invariant)
CREATE TABLE IF NOT EXISTS collection_slots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    hub_id UUID NOT NULL REFERENCES collection_hubs(id) ON DELETE CASCADE,
    stream_type waste_stream_type NOT NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    total_capacity INT NOT NULL CHECK (total_capacity > 0),
    booked_count INT NOT NULL DEFAULT 0,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_slot_capacity CHECK (booked_count >= 0 AND booked_count <= total_capacity),
    CONSTRAINT chk_slot_times CHECK (end_time > start_time)
);
CREATE INDEX IF NOT EXISTS idx_slots_hub_start ON collection_slots(hub_id, start_time);

-- 4. Bookings (Idempotent Reservation Records)
CREATE TABLE IF NOT EXISTS bookings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    slot_id UUID NOT NULL REFERENCES collection_slots(id) ON DELETE RESTRICT,
    citizen_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    stream_type waste_stream_type NOT NULL,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'CONFIRMED' CHECK (status IN ('CONFIRMED', 'CANCELLED', 'COMPLETED', 'NO_SHOW')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bookings_user ON bookings(citizen_id);
CREATE INDEX IF NOT EXISTS idx_bookings_slot ON bookings(slot_id);

-- 5. Recycling Submissions (Material Surrender & Inspection FSM)
CREATE TABLE IF NOT EXISTS recycling_submissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    qr_identity VARCHAR(128) UNIQUE NOT NULL,
    citizen_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    hub_id UUID NOT NULL REFERENCES collection_hubs(id) ON DELETE RESTRICT,
    material_type waste_stream_type NOT NULL,
    weight_kg NUMERIC(8,2) NOT NULL CHECK (weight_kg > 0),
    status VARCHAR(32) NOT NULL DEFAULT 'SCANNED' CHECK (status IN ('SCANNED', 'INSPECTION_PASSED', 'INSPECTION_FAILED', 'ESCROW_LOCKED', 'CREDITED', 'REJECTED')),
    inspector_id UUID REFERENCES users(id),
    inspection_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_submissions_qr ON recycling_submissions(qr_identity);
CREATE INDEX IF NOT EXISTS idx_submissions_citizen ON recycling_submissions(citizen_id);

-- 6. Double-Entry Ledger: Accounts
CREATE TABLE IF NOT EXISTS ledger_accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE RESTRICT,
    code VARCHAR(64) UNIQUE NOT NULL, -- e.g. 'SYSTEM_ISSUANCE_RESERVE', 'CITIZEN_WALLET:<uuid>', 'ESCROW_HOLD'
    account_type VARCHAR(32) NOT NULL CHECK (account_type IN ('ASSET', 'LIABILITY', 'EXPENSE', 'EQUITY')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 7. Double-Entry Ledger: Journal Entries (Transaction Root)
CREATE TABLE IF NOT EXISTS journal_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    description TEXT NOT NULL,
    posted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 8. Double-Entry Ledger: Individual Postings (Balanced legs)
CREATE TABLE IF NOT EXISTS ledger_postings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    journal_entry_id UUID NOT NULL REFERENCES journal_entries(id) ON DELETE RESTRICT,
    account_id UUID NOT NULL REFERENCES ledger_accounts(id) ON DELETE RESTRICT,
    amount_points BIGINT NOT NULL CHECK (amount_points > 0),
    is_credit BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_postings_account ON ledger_postings(account_id);
CREATE INDEX IF NOT EXISTS idx_postings_journal ON ledger_postings(journal_entry_id);

-- 9. API Idempotency Records (Gateway Protection)
CREATE TABLE IF NOT EXISTS idempotency_records (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    scope VARCHAR(64) NOT NULL,
    key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('PENDING', 'COMMITTED', 'FAILED')),
    response_code INT,
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_idempotency_scope_key UNIQUE (scope, key)
);

-- 10. Incidents (Geospatial & AI Triage Lifecycle)
CREATE TABLE IF NOT EXISTS incidents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    reporter_id UUID REFERENCES users(id) ON DELETE SET NULL,
    stream_type waste_stream_type NOT NULL,
    description TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    severity VARCHAR(32) NOT NULL DEFAULT 'MEDIUM' CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    status VARCHAR(32) NOT NULL DEFAULT 'RAISED' CHECK (status IN ('RAISED', 'IN_PROGRESS', 'RESOLVED', 'DISMISSED')),
    assignee_id UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_lat_lon ON incidents(latitude, longitude);

-- 11. Transactional Outbox Events (SQS / Event Streaming)
CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type VARCHAR(64) NOT NULL,
    schema_version INT NOT NULL DEFAULT 1,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(128) NOT NULL,
    correlation_id VARCHAR(128),
    payload JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events(created_at) WHERE published_at IS NULL;

-- 12. Idempotent Consumer State (Inbox / Dedup Table)
CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name VARCHAR(64) NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_name, event_id)
);

-- 13. Smart Sensor Durability State (WAL Replay Destination)
CREATE TABLE IF NOT EXISTS sensor_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sensor_id VARCHAR(64) NOT NULL,
    sequence_id BIGINT NOT NULL,
    stream_type waste_stream_type NOT NULL,
    fill_level_percent NUMERIC(5,2) NOT NULL CHECK (fill_level_percent >= 0 AND fill_level_percent <= 100),
    battery_millivolts INT,
    temperature_celsius NUMERIC(5,2),
    timestamp TIMESTAMPTZ NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_sensor_sequence UNIQUE (sensor_id, sequence_id)
);
CREATE INDEX IF NOT EXISTS idx_sensor_latest ON sensor_events(sensor_id, timestamp DESC);

-- 14. WAL Engine Checkpoints
CREATE TABLE IF NOT EXISTS wal_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    segment_file VARCHAR(255) NOT NULL,
    last_sequence_id BIGINT NOT NULL,
    checkpointed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
