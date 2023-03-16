CREATE MATERIALIZED VIEW censorship_delay_7d AS
SELECT
  COUNT(*) FILTER (WHERE blocksdelay > 0
                   AND transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0)
    AS censored_tx_count,

  AVG(delay) FILTER (WHERE blocksdelay > 0
                     AND transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0)::float
    AS censored_avg_delay,

  COUNT(*) FILTER (WHERE blocksdelay = 0) AS uncensored_tx_count,
  AVG(delay) FILTER (WHERE blocksdelay = 0)::float AS uncensored_avg_delay
  FROM
    transactions_data
 WHERE
    transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
   AND transactions_data.blacklist != '{NULL}'

WITH NO DATA;

CREATE MATERIALIZED VIEW censorship_delay_30d AS
  SELECT
  COUNT(*) FILTER (WHERE blocksdelay > 0
                   AND transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0)
  AS censored_tx_count,

  AVG(delay) FILTER (WHERE blocksdelay > 0
                     AND transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0)::float
  AS censored_avg_delay,

  COUNT(*) FILTER (WHERE blocksdelay = 0) AS uncensored_tx_count,
  AVG(delay) FILTER (WHERE blocksdelay = 0)::float AS uncensored_avg_delay
  FROM
  transactions_data
  WHERE
  transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
  AND transactions_data.blacklist != '{NULL}'

WITH NO DATA;
