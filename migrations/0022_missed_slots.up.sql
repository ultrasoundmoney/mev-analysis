DROP TABLE IF EXISTS inclusion_monitor;

CREATE TABLE missed_slots (
    id serial PRIMARY KEY,
    inserted_at timestamptz NOT NULL DEFAULT now(),
    slot_number bigint NOT NULL,
    relayed_block_hash character varying(66) NOT NULL,
    canonical_block_hash character varying(66)
);
