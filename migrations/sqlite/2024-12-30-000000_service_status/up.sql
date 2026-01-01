-- Service status tracking table for real-time visibility into running services
CREATE TABLE IF NOT EXISTS service_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_type TEXT NOT NULL,
    hostname TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'starting',
    current_source TEXT,
    started_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL,
    stats TEXT,
    last_error TEXT,
    error_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (service_type, hostname)
);

CREATE INDEX IF NOT EXISTS idx_service_status_type ON service_status(service_type);
CREATE INDEX IF NOT EXISTS idx_service_status_heartbeat ON service_status(last_heartbeat);
CREATE INDEX IF NOT EXISTS idx_service_status_source ON service_status(current_source) WHERE current_source IS NOT NULL;
