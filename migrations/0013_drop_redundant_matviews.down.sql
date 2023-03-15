CREATE MATERIALIZED VIEW IF NOT EXISTS operators_7d AS
  SELECT
    validator_pubkeys.operator_id,
    count(transactions_data.transaction_hash) AS count,
    'yes'::text AS censoring
  FROM
    transactions_data
  LEFT JOIN
    block_production
    ON block_production.block_number = transactions_data.block_number
  LEFT JOIN
    validator_pubkeys
    ON validator_pubkeys.pubkey::text = block_production.proposer_pubkey::text
  WHERE
    transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
  GROUP BY
    validator_pubkeys.operator_id
WITH NO DATA;


CREATE MATERIALIZED VIEW IF NOT EXISTS operators_30d AS
  SELECT
    validator_pubkeys.operator_id,
    count(transactions_data.transaction_hash) AS count,
    'yes'::text AS censoring
  FROM
    transactions_data
  LEFT JOIN
    block_production
    ON block_production.block_number = transactions_data.block_number
  LEFT JOIN
    validator_pubkeys
    ON validator_pubkeys.pubkey::text = block_production.proposer_pubkey::text
  WHERE
    transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
  GROUP BY
    validator_pubkeys.operator_id
WITH NO DATA;
