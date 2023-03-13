SELECT
  transactions_data.transaction_hash,
  transactions_data.block_number,
  transactions_data.mined,
  transactions_data.delay::float,
  transactions_data.blacklist,
  transactions_data.blocksdelay as block_delay
  FROM
    transactions_data
 WHERE
   (
     transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.congested
   )
   = 0
   AND transactions_data.blacklist != '{NULL}'
   AND transactions_data.blocksdelay > 0
   AND transactions_data.mined > (CURRENT_DATE - $1::interval)
