 SELECT
   'censored' AS tx_type,
   AVG(delay)::float AS avg_delay,
   COUNT(transactions_data.transaction_hash) AS tx_count
   FROM
     transactions_data
  WHERE
   transactions_data.mined > (CURRENT_DATE - $1::interval)
    AND transactions_data.blacklist != '{NULL}'
    AND transactions_data.blocksdelay > 0
  UNION
 SELECT
   'uncensored' AS tx_type,
   AVG(delay)::float AS avg_delay,
   COUNT(transactions_data.transaction_hash) AS tx_count
   FROM
     transactions_data
  WHERE
   transactions_data.mined > (CURRENT_DATE - $1::interval)
    AND transactions_data.blacklist != '{NULL}'
    AND transactions_data.blocksdelay = 0
