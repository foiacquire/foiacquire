-- Diesel schema migrations tracking table
CREATE TABLE IF NOT EXISTS __diesel_schema_migrations (
    version TEXT PRIMARY KEY NOT NULL,
    run_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
