CREATE TABLE IF NOT EXISTS upload_sessions (
    id VARCHAR(64) PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    total_bytes BIGINT NOT NULL CHECK (total_bytes > 0),
    uploaded_bytes BIGINT NOT NULL DEFAULT 0 CHECK (uploaded_bytes >= 0 AND uploaded_bytes <= total_bytes),
    status VARCHAR(32) NOT NULL DEFAULT 'INITIALIZED' CHECK (status IN ('INITIALIZED', 'UPLOADING', 'PAUSED', 'COMPLETED', 'ABORTED')),
    final_sha256 VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_upload_sessions_updated_at ON upload_sessions;
CREATE TRIGGER trg_upload_sessions_updated_at
    BEFORE UPDATE ON upload_sessions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS chunk_logs (
    id SERIAL PRIMARY KEY,
    session_id VARCHAR(64) REFERENCES upload_sessions(id) ON DELETE CASCADE,
    start_offset BIGINT NOT NULL CHECK (start_offset >= 0),
    end_offset BIGINT NOT NULL CHECK (end_offset > start_offset),
    bytes_written BIGINT NOT NULL CHECK (bytes_written = end_offset - start_offset),
    chunk_hash VARCHAR(64),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_chunk_logs_session_offset ON chunk_logs(session_id, start_offset ASC);
