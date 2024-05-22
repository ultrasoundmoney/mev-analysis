CREATE TABLE transactions (
  id bigserial PRIMARY KEY,
  address_trace varchar(42) array NOT NULL,
  block_number bigint REFERENCES blocks (block_number),
  block_timestamp timestamptz REFERENCES blocks (timestamp),
  block_timestamp_unix bigint REFERENCES blocks (timestamp_unix),
  from_address varchar(42) NOT NULL,
  gas int NOT NULL,
  gas_price bigint NOT NULL,
  input text,
  max_fee_per_gas bigint,
  max_priority_fee_per_gas bigint,
  nonce int NOT NULL,
  prev_nonce_timestamp timestamptz,
  prev_nonce_timestamp_unix bigint,
  receipt_contract_address varchar(42),
  receipt_cumulative_gas_used int NOT NULL,
  receipt_effective_gas_price bigint NOT NULL,
  receipt_gas_used int NOT NULL,
  receipt_status int NOT NULL,
  to_address varchar(42),
  transaction_hash varchar(66) UNIQUE NOT NULL,
  transaction_index int NOT NULL,
  transaction_type int NOT NULL,
  value numeric NOT NULL
);

CREATE INDEX ON transactions (prev_nonce_timestamp_unix);
