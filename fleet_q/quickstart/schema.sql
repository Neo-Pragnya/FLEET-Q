-- FLEET-Q Snowflake Schema
-- Run these statements to set up the required tables for FLEET-Q

-- ============================================================================
-- POD_HEALTH: Tracks health of all worker pods for leader election
-- ============================================================================
CREATE TABLE IF NOT EXISTS POD_HEALTH (
    pod_id VARCHAR(255) PRIMARY KEY,
    birth_ts TIMESTAMP_NTZ NOT NULL,
    last_heartbeat_ts TIMESTAMP_NTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'up',
    meta VARIANT,
    CONSTRAINT chk_status CHECK (status IN ('up', 'down'))
);

-- Index for fast leader election queries (oldest pod among alive ones)
CREATE INDEX IF NOT EXISTS idx_pod_health_leader ON POD_HEALTH(status, birth_ts);

-- Index for stale heartbeat detection
CREATE INDEX IF NOT EXISTS idx_pod_health_heartbeat ON POD_HEALTH(last_heartbeat_ts, status);


-- ============================================================================
-- STEP_TRACKER: The global task queue
-- ============================================================================
CREATE TABLE IF NOT EXISTS STEP_TRACKER (
    step_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    claimed_by VARCHAR(255),
    last_update_ts TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    payload VARIANT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    priority INT NOT NULL DEFAULT 0,
    created_ts TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT chk_step_status CHECK (status IN ('pending', 'claimed', 'completed', 'failed'))
);

-- Index for efficient pending step queries with priority
CREATE INDEX IF NOT EXISTS idx_step_pending ON STEP_TRACKER(status, priority DESC, created_ts ASC);

-- Index for finding steps claimed by specific pods
CREATE INDEX IF NOT EXISTS idx_step_claimed_by ON STEP_TRACKER(claimed_by, status);

-- Index for tracking step lifecycle
CREATE INDEX IF NOT EXISTS idx_step_lifecycle ON STEP_TRACKER(status, last_update_ts);


-- ============================================================================
-- Example: Insert test data (optional)
-- ============================================================================

-- Sample pod (you would not insert this manually in production)
-- INSERT INTO POD_HEALTH (pod_id, birth_ts, last_heartbeat_ts, status, meta)
-- VALUES ('pod-001', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'up', 
--         PARSE_JSON('{"cpu_cores": 4, "threads": 8, "version": "1.0.0"}'));

-- Sample step
-- INSERT INTO STEP_TRACKER (step_id, status, payload, priority)
-- VALUES ('step-001', 'pending', 
--         PARSE_JSON('{"task_type": "process_data", "args": {"file": "data.csv"}}'),
--         1);
