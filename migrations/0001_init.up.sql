CREATE TABLE blocks (
  id                     BIGSERIAL PRIMARY KEY,

  timestamp              TIMESTAMPTZ NOT NULL,
  block_number           BIGINT UNIQUE NOT NULL,
  block_hash             VARCHAR(66) NOT NULL,

  fee_recipient          VARCHAR(42) NOT NULL,
  extra_data             TEXT,

  tx_count               INT NOT NULL,
  gas_limit              BIGINT NOT NULL,
  gas_used               BIGINT NOT NULL,
  base_fee_per_gas       BIGINT NOT NULL
);

CREATE TABLE txs (
	id              BIGSERIAL PRIMARY KEY,
  tx_hash         VARCHAR(66) NOT NULL,
  tx_index        INT NOT NULL,
  block_number    BIGINT REFERENCES blocks (block_number),
  first_seen_at   TIMESTAMPTZ,
  max_fee         NUMERIC,
  max_prio_fee    NUMERIC,
  address_trace   VARCHAR(42) ARRAY NOT NULL
);

CREATE TABLE block_production (
  id              BIGSERIAL PRIMARY KEY,
  block_number    BIGINT UNIQUE,
  builder_pubkey  VARCHAR(98),
  proposer_pubkey VARCHAR(98),
  relay_pubkeys   VARCHAR(98) ARRAY NOT NULL
);

CREATE TABLE blacklists (
  id           TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  address_list VARCHAR(42) ARRAY NOT NULL
);

CREATE TABLE tx_blacklists (
  tx_id        BIGINT REFERENCES txs (id),
  blacklist_id TEXT REFERENCES blacklists (id)
);
