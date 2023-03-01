CREATE TABLE blocks (
  id bigserial PRIMARY KEY,
  base_fee_per_gas bigint NOT NULL,
  block_hash varchar(66) UNIQUE NOT NULL,
  block_number bigint UNIQUE NOT NULL,
  extra_data text,
  fee_recipient varchar(42) NOT NULL,
  gas_limit bigint NOT NULL,
  gas_used bigint NOT NULL,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  logs_bloom varchar(514) NOT NULL,
  parent_hash varchar(66) NOT NULL,
  receipts_root varchar(66) NOT NULL,
  sha3_uncles varchar(66) NOT NULL,
  size int NOT NULL,
  state_root varchar(66) NOT NULL,
  timestamp timestamptz NOT NULL UNIQUE,
  timestamp_unix bigint NOT NULL UNIQUE,
  transaction_count int NOT NULL,
  transactions_root varchar(66) NOT NULL
);

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

CREATE TABLE mempool_timestamps (
  transaction_hash varchar(66) REFERENCES transactions (transaction_hash),
  source_id varchar(20) NOT NULL,
  timestamp timestamptz NOT NULL,
  timestamp_unix bigint NOT NULL,
  UNIQUE (transaction_hash, source_id)
);

CREATE INDEX ON mempool_timestamps (timestamp_unix);

CREATE TYPE blacklist_id_enum AS ENUM ('ofac', 'uk');

CREATE TABLE blacklist_entries (
  address varchar(42) PRIMARY KEY,
  display_name text NOT NULL,
  blacklist_id blacklist_id_enum NOT NULL,
  date_added date NOT NULL,
  date_added_unix bigint NOT NULL,
  UNIQUE (address, blacklist_id)
);

CREATE INDEX ON blacklist_entries (date_added_unix);

CREATE TABLE transaction_blacklists (
  transaction_hash varchar(66) REFERENCES transactions (transaction_hash),
  blacklist_id blacklist_id_enum NOT NULL
);

CREATE TABLE block_production (
  id bigserial PRIMARY KEY,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  slot_number bigint NOT NULL,
  block_number bigint NOT NULL,
  block_hash varchar(66) NOT NULL,
  builder_pubkey varchar(98),
  proposer_pubkey varchar(98),
  relays text array,
  UNIQUE (slot_number, block_number, block_hash)
);

CREATE TABLE builder_pubkeys (
  id bigserial PRIMARY KEY,
  pubkey varchar(98) NOT NULL,
  builder_id text NOT NULL
);
