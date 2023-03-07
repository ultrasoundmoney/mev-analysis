DROP MATERIALIZED VIEW IF EXISTS builders_7d;
DROP MATERIALIZED VIEW IF EXISTS builders_30d;
DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_7d;
DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_30d;

CREATE MATERIALIZED VIEW builders_7d AS
SELECT
	bID,
	SUM(NEST.nOfBlocks) - SUM(NEST.notc) AS censoring_pubkeys,
	SUM(NEST.nOfBlocks) AS total_pubkeys,
	MAX(bc.block_count) AS number_of_blocks
FROM (
	SELECT
		builder_pubkeys.builder_id AS bID,
		CASE WHEN uncensored = 1 THEN
			1
		ELSE
			0
		END AS notc,
		1 AS nOfBlocks
	FROM
		builder_pubkeys
	LEFT JOIN ( SELECT DISTINCT
			(builder_pubkey),
			builder_pubkeys.builder_id,
			uncensored
		FROM ( SELECT DISTINCT
				(transactions_data.block_number),
				blocks.block_hash,
				block_production.builder_pubkey,
				1 AS uncensored
			FROM
				transactions_data
				LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
				LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
			WHERE
				transactions_data.blacklist <> '{NULL}' AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)) A_NEST
		LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey) B ON B.builder_pubkey = builder_pubkeys.pubkey) NEST
		LEFT JOIN (
		SELECT COUNT(distinct(transactions_data.block_number)) AS block_count, builder_pubkeys.builder_id
FROM transactions_data

LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
WHERE transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
GROUP BY builder_pubkeys.builder_id) bc ON bc.builder_id=bID
GROUP BY
	bID
WITH NO DATA;


CREATE MATERIALIZED VIEW builders_30d AS
SELECT
	bID,
	sum(NEST.nOfBlocks) - sum(NEST.notc) AS censoring_pubkeys,
	sum(NEST.nOfBlocks) AS total_pubkeys,
	max(bc.block_count) AS number_of_blocks
FROM (
	SELECT
		builder_pubkeys.builder_id AS bID,
		CASE WHEN uncensored = 1 THEN
			1
		ELSE
			0
		END AS notc,
		1 AS nOfBlocks
	FROM
		builder_pubkeys
	LEFT JOIN ( SELECT DISTINCT
			(builder_pubkey),
			builder_pubkeys.builder_id,
			uncensored
		FROM ( SELECT DISTINCT
				(transactions_data.block_number),
				blocks.block_hash,
				block_production.builder_pubkey,
				1 AS uncensored
			FROM
				transactions_data
				LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
				LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
			WHERE
				transactions_data.blacklist <> '{NULL}' AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)) A_NEST
		LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey) B ON B.builder_pubkey = builder_pubkeys.pubkey) NEST
		LEFT JOIN (
		SELECT COUNT(DISTINCT(transactions_data.block_number)) AS block_count, builder_pubkeys.builder_id
FROM transactions_data

LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
WHERE transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
GROUP BY builder_pubkeys.builder_id) bc ON bc.builder_id=bID
GROUP BY
	bID
WITH NO DATA;


CREATE MATERIALIZED VIEW inclusion_delay_7d AS
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'ofac'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.blocksdelay > 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'normal'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.lowbasefee = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.minertransaction) = 0
	AND transactions_data.lowtip = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip) = 0
	AND transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.congested = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip) = 0
	AND transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction + transactions_data.congested) = 0
	AND transactions_data.blocksdelay > 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'next_block'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction + transactions_data.congested) = 0
	AND transactions_data.blocksdelay = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
WITH NO DATA;


CREATE MATERIALIZED VIEW inclusion_delay_30d AS
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'ofac'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist <> '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.blocksdelay > 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'normal'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.lowbasefee = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.minertransaction) = 0
	AND transactions_data.lowtip = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip) = 0
	AND transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction) = 0
	AND transactions_data.congested = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.congested + transactions_data.lowbasefee + transactions_data.lowtip) = 0
	AND transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction + transactions_data.congested) = 0
	AND transactions_data.blocksdelay > 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	AVG(transactions_data.delay) AS avg_delay,
	AVG(transactions_data.blocksdelay) AS avg_block_delay,
	COUNT(transactions_data.transaction_hash) AS n,
	'next_block'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.blacklist = '{NULL}'::text[]
	AND (transactions_data.lowbasefee + transactions_data.lowtip + transactions_data.minertransaction + transactions_data.congested) = 0
	AND transactions_data.blocksdelay = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
WITH NO DATA;
