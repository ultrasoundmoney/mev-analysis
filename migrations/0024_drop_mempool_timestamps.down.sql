CREATE TABLE mempool_timestamps (
  transaction_hash varchar(66) REFERENCES transactions (transaction_hash),
  source_id varchar(20) NOT NULL,
  timestamp timestamptz NOT NULL,
  timestamp_unix bigint NOT NULL,
  UNIQUE (transaction_hash, source_id)
);

CREATE INDEX ON mempool_timestamps (timestamp_unix);
