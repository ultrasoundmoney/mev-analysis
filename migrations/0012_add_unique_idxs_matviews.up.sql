CREATE UNIQUE INDEX IF NOT EXISTS builders_7d_bid ON builders_7d (bid);
CREATE UNIQUE INDEX IF NOT EXISTS builders_30d_bid ON builders_30d (bid);

CREATE UNIQUE INDEX IF NOT EXISTS builder_blocks_7d_builder_pubkey ON builder_blocks_7d (builder_pubkey);
CREATE UNIQUE INDEX IF NOT EXISTS builder_blocks_30d_builder_pubkey ON builder_blocks_30d (builder_pubkey);

CREATE UNIQUE INDEX IF NOT EXISTS censored_transactions_7d_transaction_hash ON censored_transactions_7d (transaction_hash);
CREATE UNIQUE INDEX IF NOT EXISTS censored_transactions_30d_transaction_hash ON censored_transactions_30d (transaction_hash);

CREATE UNIQUE INDEX IF NOT EXISTS inclusion_delay_7d_t_type ON inclusion_delay_7d (t_type);
CREATE UNIQUE INDEX IF NOT EXISTS inclusion_delay_30d_t_type ON inclusion_delay_30d (t_type);

CREATE UNIQUE INDEX IF NOT EXISTS operators_all_operator_id ON operators_all (operator_id);

CREATE UNIQUE INDEX IF NOT EXISTS top_7d_transaction_hash ON top_7d (transaction_hash);
CREATE UNIQUE INDEX IF NOT EXISTS top_30d_transaction_hash ON top_30d (transaction_hash);
