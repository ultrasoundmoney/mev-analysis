DROP MATERIALIZED VIEW IF EXISTS builder_blocks_7d;
DROP MATERIALIZED VIEW IF EXISTS builder_blocks_30d;

CREATE MATERIALIZED VIEW IF NOT EXISTS builder_blocks_7d AS
SELECT DISTINCT
(builder_pubkeys.pubkey) AS builder_pubkey,
   COALESCE(SUM(uncensored), 0) AS uncensored_blocks,
   MIN(builder_pubkeys.builder_name) AS builder_name,
   COALESCE(MIN(total_blocks), 0) AS total_blocks
FROM
   (
      SELECT DISTINCT
         transactions_data.block_number,
         blocks.block_hash,
         block_production.builder_pubkey,
         1 AS uncensored
      FROM
         transactions_data
         LEFT JOIN
            blocks
            ON blocks.block_number = transactions_data.block_number
         LEFT JOIN
            block_production
            ON block_production.block_hash::text = blocks.block_hash::text
      WHERE
         transactions_data.blacklist != '{NULL}'::text[]
         AND blocks.timestamp > (CURRENT_DATE - '7 days'::interval)
   )
   "NEST"
   RIGHT JOIN
      builder_pubkeys
      ON builder_pubkeys.pubkey = builder_pubkey
   LEFT JOIN
      (
         SELECT DISTINCT
            COUNT(blocks.block_hash) AS total_blocks,
            block_production.builder_pubkey AS pubkey
         FROM
            blocks
            LEFT JOIN
               block_production
               ON block_production.block_hash::text = blocks.block_hash::text
         WHERE
            blocks.timestamp > (CURRENT_DATE - '7 days'::interval)
         GROUP BY
            block_production.builder_pubkey
      )
      total
      ON total.pubkey = builder_pubkeys.pubkey
GROUP BY
   builder_pubkeys.pubkey WITH NO DATA;


CREATE MATERIALIZED VIEW IF NOT EXISTS builder_blocks_30d AS
SELECT DISTINCT
(builder_pubkeys.pubkey) AS builder_pubkey,
   COALESCE(SUM(uncensored), 0) AS uncensored_blocks,
   MIN(builder_pubkeys.builder_name) AS builder_name,
   COALESCE(MIN(total_blocks), 0) AS total_blocks
FROM
   (
      SELECT DISTINCT
         transactions_data.block_number,
         blocks.block_hash,
         block_production.builder_pubkey,
         1 AS uncensored
      FROM
         transactions_data
         LEFT JOIN
            blocks
            ON blocks.block_number = transactions_data.block_number
         LEFT JOIN
            block_production
            ON block_production.block_hash::text = blocks.block_hash::text
      WHERE
         transactions_data.blacklist != '{NULL}'::text[]
         AND blocks.timestamp > (CURRENT_DATE - '30 days'::interval)
   )
   "NEST"
   RIGHT JOIN
      builder_pubkeys
      ON builder_pubkeys.pubkey = builder_pubkey
   LEFT JOIN
      (
         SELECT DISTINCT
            COUNT(blocks.block_hash) AS total_blocks,
            block_production.builder_pubkey AS pubkey
         FROM
            blocks
            LEFT JOIN
               block_production
               ON block_production.block_hash::text = blocks.block_hash::text
         WHERE
            blocks.timestamp > (CURRENT_DATE - '30 days'::interval)
         GROUP BY
            block_production.builder_pubkey
      )
      total
      ON total.pubkey = builder_pubkeys.pubkey
GROUP BY
   builder_pubkeys.pubkey WITH NO DATA;
