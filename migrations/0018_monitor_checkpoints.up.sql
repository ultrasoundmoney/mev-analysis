CREATE TABLE monitor_checkpoints (
    id serial PRIMARY KEY,
    monitor_id text UNIQUE NOT NULL,
    timestamp timestamptz NOT NULL
);
