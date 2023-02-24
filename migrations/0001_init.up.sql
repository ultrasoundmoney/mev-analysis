CREATE TABLE blocks (
  id bigserial PRIMARY KEY,
  timestamp timestamptz NOT NULL,
  block_number bigint UNIQUE NOT NULL,
  block_hash varchar(66) UNIQUE NOT NULL,
  fee_recipient varchar(42) NOT NULL,
  extra_data text,
  tx_count int NOT NULL,
  gas_limit bigint NOT NULL,
  gas_used bigint NOT NULL,
  base_fee_per_gas bigint NOT NULL
);

CREATE TABLE txs (
  id bigserial PRIMARY KEY,
  tx_hash varchar(66) UNIQUE NOT NULL,
  tx_index int NOT NULL,
  block_number bigint REFERENCES blocks (block_number),
  base_fee numeric,
  max_prio_fee numeric,
  address_trace varchar(42) array NOT NULL
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
  block_number bigint UNIQUE NOT NULL,
  builder_pubkey varchar(98),
  proposer_pubkey varchar(98),
  relays text array
);
