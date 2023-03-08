SELECT * FROM (
SELECT
  transactions_data.transaction_hash,
  transactions_data.mined,
  transactions_data.delay::float,
  transactions_data.block_number,
  transactions_data.blocksdelay as block_delay,
  transactions_data.blacklist,
  CASE
  WHEN transactions_data.blocksdelay >= 10000 THEN 'likely_insufficient_balance'
  WHEN transactions_data.lowbasefee = 1 THEN 'lowbasefee'
  WHEN transactions_data.blocksdelay >= 100 THEN 'likely_insufficient_balance'
  WHEN transactions_data.lowtip = 1 THEN 'lowtip'
  WHEN transactions_data.congested = 1 THEN 'congested'
  WHEN transactions_data.blacklist != '{NULL}' THEN 'ofac'
  ELSE 'unknown'
  END
   AS reason
  FROM
    transactions_data
 WHERE
   transactions_data.mined > (CURRENT_DATE - $1::interval)
   AND transactions_data.blocksdelay > 1) NESTED
   WHERE reason='ofac' or reason='unknown'
