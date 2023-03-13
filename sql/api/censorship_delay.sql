SELECT
  COUNT(*) FILTER (WHERE blocksdelay > 1 and transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0 ) AS censored_tx_count,
  AVG(delay) FILTER (WHERE blocksdelay > 1 and transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested = 0)::float AS censored_avg_delay,
  COUNT(*) FILTER (WHERE blocksdelay <= 1) AS uncensored_tx_count,
  AVG(delay) FILTER (WHERE blocksdelay <= 1)::float AS uncensored_avg_delay
FROM
  transactions_data
WHERE
    transactions_data.mined > (CURRENT_DATE - $1::interval)
    AND transactions_data.blacklist != '{NULL}'
