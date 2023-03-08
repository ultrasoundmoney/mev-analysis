SELECT
   *
FROM
   (
      SELECT
         transactions_data.transaction_hash,
         transactions_data.mined,
         transactions_data.delay::float,
         transactions_data.block_number,
         transactions_data.blocksdelay as block_delay,
         CASE
            WHEN blacklist = '{NULL}' THEN '{}'::text[]
            ELSE blacklist
         END AS blacklist,
         CASE
            WHEN transactions_data.blocksdelay >= 10000 THEN 'likely_insufficient_balance'
            WHEN transactions_data.lowbasefee = 1 THEN 'lowbasefee'
            WHEN transactions_data.blocksdelay >= 100 THEN 'likely_insufficient_balance'
            WHEN transactions_data.lowtip = 1 THEN 'lowtip'
            WHEN transactions_data.congested = 1 THEN 'congested'
            WHEN transactions_data.blacklist != '{NULL}' THEN 'ofac'
            ELSE 'unknown'
         END AS reason
      FROM
         transactions_data
      WHERE
         transactions_data.mined > (CURRENT_DATE - $1::interval)
         AND transactions_data.blocksdelay > 1
   ) sq
WHERE
   reason = 'ofac'
   OR reason = 'unknown'
