CREATE TABLE inclusion_monitor (
  id bigserial PRIMARY KEY,
  inserted_at timestamptz NOT NULL DEFAULT now(),
  slot_number bigint UNIQUE NOT NULL,
  relayed_block_hash varchar(66) NOT NULL,
  canonical_block_hash varchar(66) NOT NULL
);
