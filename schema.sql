CREATE TABLE IF NOT EXISTS jobs(
    id UUID PRIMARY KEY DEFAULT gen_random_UUID(),
    stream_id TEXT UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('pending','processing','completed','failed')),
    payload JSONB,
    worker_id TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error TEXT,
    attempts INT DEFAULT 0
);