
CREATE MATERIALIZED VIEW IF NOT EXISTS builders_pubkey_7d AS SELECT DISTINCT
	(builder_pubkeys.pubkey) AS "builder_pubkey",
	coalesce(sum(uncensored), 0) AS "uncensored_blocks",
	min(builder_pubkeys.builder_id) AS "builder_id",
	coalesce(min(total_blocks), 0)
FROM ( SELECT DISTINCT
		transactions_data.block_number,
		blocks.block_hash,
		block_production.builder_pubkey,
		1 AS uncensored
	FROM
		transactions_data
	LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
	LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)) "NEST"
	RIGHT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
	LEFT JOIN ( SELECT DISTINCT
			count(blocks.block_hash) AS "total_blocks",
			block_production.builder_pubkey AS "pubkey"
		FROM
			transactions_data
			LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
			LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
		WHERE
			transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
		GROUP BY
			block_production.builder_pubkey) "total" ON "total".pubkey = builder_pubkeys.pubkey
GROUP BY
	builder_pubkeys.pubkey
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS builders_pubkey_30d AS SELECT DISTINCT
	(builder_pubkeys.pubkey) AS "builder_pubkey",
	coalesce(sum(uncensored), 0) AS "uncensored_blocks",
	min(builder_pubkeys.builder_id) AS "builder_id",
	coalesce(min(total_blocks), 0)
FROM ( SELECT DISTINCT
		transactions_data.block_number,
		blocks.block_hash,
		block_production.builder_pubkey,
		1 AS uncensored
	FROM
		transactions_data
	LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
	LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)) "NEST"
	RIGHT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
	LEFT JOIN ( SELECT DISTINCT
			count(blocks.block_hash) AS "total_blocks",
			block_production.builder_pubkey AS "pubkey"
		FROM
			transactions_data
			LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
			LEFT JOIN block_production ON block_production.block_hash::text = blocks.block_hash::text
		WHERE
			transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
		GROUP BY
			block_production.builder_pubkey) "total" ON "total".pubkey = builder_pubkeys.pubkey
GROUP BY
	builder_pubkeys.pubkey
WITH NO DATA;
