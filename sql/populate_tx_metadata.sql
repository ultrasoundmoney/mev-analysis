WITH inserted_rows as (
INSERT INTO
   transactions_data
   SELECT
      "NEST2".transaction_hash,
      min("NEST2".tbn) AS blocknumber,
      min("NEST2".minertx) AS minertransaction,
      max("NEST2".lowbasefee) AS lowbasefee,
      max("NEST2".congestion) AS congested,
      CASE
         WHEN
            count(DISTINCT "NEST2".block_number) > 0
            AND max("NEST2".lowbasefee) < 1
         THEN
            max("NEST2".lowfeetip)
         ELSE
            0
      END
      AS lowtip, min("NEST2".blockts) AS mined, max("NEST2".delay) AS delay, array_agg(DISTINCT "NEST2".blacklist) AS blacklist, count(DISTINCT "NEST2".block_number) AS blocksdelay
   FROM
      (
         SELECT
            "NEST".minertx,
            "NEST".tbn,
            "NEST".blockts,
            "NEST".prevts,
            "NEST".transaction_hash,
            blocks.block_number,
            CASE
               WHEN
                  "NEST".bf < blocks.base_fee_per_gas
               THEN
                  1
               ELSE
                  0
            END
            AS lowbasefee,
            CASE
               WHEN
                  (
                     "NEST".bf - blocks.base_fee_per_gas
                  )
                  >= 1000000000 AND "NEST".pf>=1000000000 and tt=2
               THEN
                  0
            WHEN
                    (
                     "NEST".bf - blocks.base_fee_per_gas
                  )
                  >= 1000000000 AND tt=0
                  THEN 0
               ELSE
                  1
            END
            AS lowfeetip, blocks.gas_limit AS gaslimit, blocks.gas_used AS gasused, "NEST".gas,
            CASE
               WHEN
                  "NEST".gas > (blocks.gas_limit - blocks.gas_used)
               THEN
                  1
               ELSE
                  0
            END
            AS congestion, "NEST".delay, "NEST".blacklist,
            9 as low_balance
         FROM
            (
               SELECT
                  transactions.transaction_hash,
                  min(transactions.block_number) AS tbn,
                  CASE
                     WHEN
                        min(transactions.max_fee_per_gas) IS NOT NULL
                     THEN
                        min(transactions.max_fee_per_gas)
                     ELSE
                        min(transactions.gas_price)
                  END
                  AS bf,
                  min(transactions.max_priority_fee_per_gas) as pf,
                  min (transactions.transaction_type) as tt,
                   min(transactions.gas) AS gas, min(blocks_1."timestamp") AS blockts, min(mempool_timestamps."timestamp") AS memts,
                  CASE
                     WHEN
                        (
                           min(extract(epoch
                        FROM
                           blocks_1."timestamp")) - ((
                           SELECT
                              percentile_cont(0.5) WITHIN GROUP (
                           ORDER BY
(mempool_timestamps.timestamp_unix)) AS percentile_cont))
                        )
                        <= 0
                     THEN
                        0
                     ELSE
                        min(extract(epoch
               FROM
                  blocks_1."timestamp")) - greatest(extract(epoch
               FROM
                  min(transactions.prev_nonce_timestamp)),
                  (
(
                     SELECT
                        percentile_cont(0.5) WITHIN GROUP (
                     ORDER BY
(mempool_timestamps.timestamp_unix)) AS percentile_cont)
                  )
)
                  END
                  AS delay,
                  CASE
                     WHEN
                        (
                           min(extract(epoch
                        FROM
                           blocks_1."timestamp")) - ((
                           SELECT
                              percentile_cont(0.5) WITHIN GROUP (
                           ORDER BY
(mempool_timestamps.timestamp_unix)) AS percentile_cont))
                        )
                        <= 0
                     THEN
                        1
                     ELSE
                        0
                  END
                  AS minertx, min(blocks_1.block_number) AS bn, max(transaction_blacklists.blacklist_id) AS blacklist, min(transactions.prev_nonce_timestamp) AS prevts
               FROM
                  transactions
                  LEFT JOIN
                     blocks blocks_1
                     ON blocks_1.block_number = transactions.block_number
                  LEFT JOIN
                     (
                        SELECT
                           "NEST2_1".transaction_hash,
                           min("NEST2_1".blacklist_id) AS blacklist_id
                        FROM
                           (
                              SELECT
                                 "NEST_1".transaction_hash,
                                 "NEST_1".trace,
                                 blacklist_entries.display_name,
                                 blacklist_entries.blacklist_id
                              FROM
                                 (
                                    SELECT
                                       transactions_1.transaction_hash,
                                       unnest(transactions_1.address_trace) AS trace,
                                       transactions_1.block_timestamp
                                    FROM
                                       transactions transactions_1
                                 )
                                 "NEST_1"
                                 LEFT JOIN
                                    blacklist_entries
                                    ON blacklist_entries.address = "NEST_1".trace
                              WHERE
                                 (
                                    "NEST_1".trace IN
                                    (
                                       SELECT
                                          blacklist_entries_1.address
                                       FROM
                                          blacklist_entries blacklist_entries_1
                                    )
                                 )
                                 AND "NEST_1".block_timestamp > blacklist_entries.date_added
                           )
                           "NEST2_1"
                        GROUP BY
                           "NEST2_1".transaction_hash
                     )
                     transaction_blacklists
                     ON transaction_blacklists.transaction_hash = transactions.transaction_hash
                  LEFT JOIN
                     mempool_timestamps
                     ON mempool_timestamps.transaction_hash = transactions.transaction_hash
               WHERE
                  transactions.block_timestamp > (
                  SELECT
                     GREATEST('2020-01-01', MAX(transactions_data.mined))
                  from
                     transactions_data)
                  GROUP BY
                     transactions.transaction_hash,
                     blocks_1.gas_limit,
                     blocks_1.gas_used
            )
            "NEST"
            LEFT JOIN
               blocks
               ON blocks."timestamp" > GREATEST ("NEST".memts, "NEST".prevts)
               AND blocks."timestamp" < "NEST".blockts
      )
      "NEST2"
   GROUP BY
      "NEST2".transaction_hash
   RETURNING 1
)

SELECT COUNT(*) FROM inserted_rows;
