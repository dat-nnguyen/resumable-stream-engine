-- PostgreSQL schema for upload sessions and chunk logs
CREATE TABLE IF NOT EXISTS upload_sessions (
    id VARCHAR(64) PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    total_bytes BIGINT NOT NULL,
    uploaded_bytes BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'INITIALIZED',
    final_sha256 VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS chunk_logs (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(64) REFERENCES upload_sessions(id) ON DELETE CASCADE,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    bytes_written BIGINT NOT NULL,
    chunk_hash VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chunk_session ON chunk_logs(session_id);
