CREATE TABLE validator_pubkeys (
    id serial PRIMARY KEY,
    inserted_at timestamptz NOT NULL DEFAULT now(),
    pubkey varchar(98) UNIQUE NOT NULL,
    validator_index int UNIQUE NOT NULL,
    operator_id text NOT NULL
);
