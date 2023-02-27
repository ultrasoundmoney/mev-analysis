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
  tx_count int NOT NULL,
  txs_root varchar(66) NOT NULL
);

CREATE TABLE txs (
  id bigserial PRIMARY KEY,
  address_trace varchar(42) array NOT NULL,
  block_number bigint REFERENCES blocks (block_number),
  block_timestamp timestamptz REFERENCES blocks (timestamp),
  from_address varchar(42) NOT NULL,
  gas int NOT NULL,
  gas_price bigint NOT NULL,
  input text,
  max_fee_per_gas bigint,
  max_priority_fee_per_gas bigint,
  nonce int NOT NULL,
  receipt_contract_address varchar(42),
  receipt_cumulative_gas_used int NOT NULL,
  receipt_effective_gas_price bigint NOT NULL,
  receipt_gas_used int NOT NULL,
  receipt_status int NOT NULL,
  to_address varchar(42),
  tx_hash varchar(66) UNIQUE NOT NULL,
  tx_index int NOT NULL,
  tx_type int NOT NULL,
  value numeric NOT NULL
);

CREATE TABLE mempool_timestamps (
  tx_hash varchar(66) REFERENCES txs (tx_hash),
  source_id varchar(20) NOT NULL,
  timestamp timestamptz NOT NULL,
  UNIQUE (tx_hash, source_id)
);

CREATE TABLE blacklists (
  id text PRIMARY KEY,
  display_name text NOT NULL,
  address_list varchar(42) array NOT NULL
);

CREATE TABLE tx_blacklists (
  tx_id bigint REFERENCES txs (id),
  blacklist_id text REFERENCES blacklists (id)
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
