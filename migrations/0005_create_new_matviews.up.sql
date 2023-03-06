CREATE MATERIALIZED VIEW test_builders_7d AS  SELECT c.bid,
    sum(c.n) - sum(c.notc) AS censoring_pubkeys,
    sum(c.n) AS total_pubkeys,
    max(bc.block_count) AS number_of_blocks
   FROM ( SELECT builder_pubkeys.builder_id AS bid,
                CASE
                    WHEN b.uncensored = 1 THEN 1
                    ELSE 0
                END AS notc,
            1 AS n
           FROM builder_pubkeys
             LEFT JOIN ( SELECT DISTINCT a.builder_pubkey,
                    builder_pubkeys_1.builder_id,
                    a.uncensored
                   FROM ( SELECT DISTINCT transactions_data.block_number,
                            blocks.block_hash,
                            block_production.builder_pubkey,
                            1 AS uncensored
                           FROM transactions_data
                             LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
                             LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
                          WHERE transactions_data.blacklist <> '{NULL}'::text[] AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)) a
                     LEFT JOIN builder_pubkeys builder_pubkeys_1 ON builder_pubkeys_1.pubkey::text = a.builder_pubkey::text) b ON b.builder_pubkey::text = builder_pubkeys.pubkey::text) c
     LEFT JOIN ( SELECT count(DISTINCT transactions_data.block_number) AS block_count,
            builder_pubkeys.builder_id
           FROM transactions_data
             LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
             LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
             LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey::text = block_production.builder_pubkey::text
          WHERE transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
          GROUP BY builder_pubkeys.builder_id) bc ON bc.builder_id = c.bid
  GROUP BY c.bid;

CREATE MATERIALIZED VIEW test_builders_1m AS  SELECT c.bid,
    sum(c.n) - sum(c.notc) AS censoring_pubkeys,
    sum(c.n) AS total_pubkeys,
    max(bc.block_count) AS number_of_blocks
   FROM ( SELECT builder_pubkeys.builder_id AS bid,
                CASE
                    WHEN b.uncensored = 1 THEN 1
                    ELSE 0
                END AS notc,
            1 AS n
           FROM builder_pubkeys
             LEFT JOIN ( SELECT DISTINCT a.builder_pubkey,
                    builder_pubkeys_1.builder_id,
                    a.uncensored
                   FROM ( SELECT DISTINCT transactions_data.block_number,
                            blocks.block_hash,
                            block_production.builder_pubkey,
                            1 AS uncensored
                           FROM transactions_data
                             LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
                             LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
                          WHERE transactions_data.blacklist <> '{NULL}'::text[] AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)) a
                     LEFT JOIN builder_pubkeys builder_pubkeys_1 ON builder_pubkeys_1.pubkey::text = a.builder_pubkey::text) b ON b.builder_pubkey::text = builder_pubkeys.pubkey::text) c
     LEFT JOIN ( SELECT count(DISTINCT transactions_data.block_number) AS block_count,
            builder_pubkeys.builder_id
           FROM transactions_data
             LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
             LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
             LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey::text = block_production.builder_pubkey::text
          WHERE transactions_data.mined > (CURRENT_DATE - '7 mon'::interval)
          GROUP BY builder_pubkeys.builder_id) bc ON bc.builder_id = c.bid
  GROUP BY c.bid;
