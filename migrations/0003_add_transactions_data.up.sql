CREATE TABLE IF NOT EXISTS transactions_data (
  transaction_hash text PRIMARY KEY,
  block_number bigint NOT NULL,
  minertransaction int NOT NULL,
  lowbasefee int NOT NULL,
  congested int NOT NULL,
  lowtip int NOT NULL,
  mined timestamptz NOT NULL,
  delay numeric,
  blacklist text[] NOT NULL,
  blocksdelay int NOT NULL
);

CREATE INDEX IF NOT EXISTS transactions_data_mined_idx ON transactions_data (mined);
