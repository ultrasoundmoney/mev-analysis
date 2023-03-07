CREATE MATERIALIZED VIEW operators_all AS
  SELECT
  nested.operator_id,
  COUNT(DISTINCT nested.pubkey) AS validator_count,
  ARRAY_AGG(DISTINCT nested.relays) AS relays
  FROM
  (
    SELECT
      validator_pubkeys.pubkey,
      validator_pubkeys.operator_id,
      UNNEST(block_production.relays) AS relays
      FROM
        validator_pubkeys
        LEFT JOIN
        block_production
            ON block_production.proposer_pubkey::text = validator_pubkeys.pubkey::text
  )
  nested
  GROUP BY
  nested.operator_id
  WITH NO DATA;
