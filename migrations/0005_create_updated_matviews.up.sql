DROP MATERIALIZED VIEW IF EXISTS builders_7d;
DROP MATERIALIZED VIEW IF EXISTS builders_30d;
DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_7d;
DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_30d;

CREATE MATERIALIZED VIEW builders_7d AS SELECT
	bID,
	sum(NEST.nOfBlocks) - sum(NEST.notc) AS censoring_pubkeys,
	sum(NEST.nOfBlocks) AS total_pubkeys,
	max(bc.block_count) as number_of_blocks
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
				transactions_data.blacklist <> '{NULL}' and transactions_data.mined > (CURRENT_DATE - '7 days'::interval)) A_NEST
		LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey) B ON B.builder_pubkey = builder_pubkeys.pubkey) NEST
		LEFT JOIN (
		SELECT count(distinct(transactions_data.block_number)) as block_count, builder_pubkeys.builder_id
FROM transactions_data

LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
WHERE transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
group by builder_pubkeys.builder_id) bc on bc.builder_id=bID
GROUP BY
	bID
WITH NO DATA;
  

CREATE MATERIALIZED VIEW builders_30d AS SELECT
	bID,
	sum(NEST.nOfBlocks) - sum(NEST.notc) AS censoring_pubkeys,
	sum(NEST.nOfBlocks) AS total_pubkeys,
	max(bc.block_count) as number_of_blocks
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
				transactions_data.blacklist <> '{NULL}' and transactions_data.mined > (CURRENT_DATE - '1 mon'::interval)) A_NEST
		LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey) B ON B.builder_pubkey = builder_pubkeys.pubkey) NEST
		LEFT JOIN (
		SELECT count(distinct(transactions_data.block_number)) as block_count, builder_pubkeys.builder_id
FROM transactions_data

LEFT JOIN blocks ON blocks.block_number = transactions_data.block_number
LEFT JOIN block_production ON block_production.block_hash = blocks.block_hash
LEFT JOIN builder_pubkeys ON builder_pubkeys.pubkey = builder_pubkey
WHERE transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
group by builder_pubkeys.builder_id) bc on bc.builder_id=bID
GROUP BY
	bID
WITH NO DATA;  

  CREATE MATERIALIZED VIEW inclusion_delay_7d AS SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'ofac'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist <> '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist <> '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.blocksdelay > 0
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'normal'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.lowbasefee = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.lowtip = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip) = 0
	AND transactions_data_2.minertransaction = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.congested = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip) = 0
	AND transactions_data_2.minertransaction = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction + transactions_data_2.congested) = 0
	AND transactions_data_2.blocksdelay > 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'next_block'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction + transactions_data_2.congested) = 0
	AND transactions_data_2.blocksdelay = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '7 days'::interval)
WITH NO DATA;


SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'ofac'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist <> '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist <> '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.blocksdelay > 0
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'normal'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.lowbasefee = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.lowtip = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip) = 0
	AND transactions_data_2.minertransaction = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction) = 0
	AND transactions_data_2.congested = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.congested + transactions_data_2.lowbasefee + transactions_data_2.lowtip) = 0
	AND transactions_data_2.minertransaction = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction + transactions_data_2.congested) = 0
	AND transactions_data_2.blocksdelay > 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data_2.delay) AS avg_delay,
	avg(transactions_data_2.blocksdelay) AS avg_block_delay,
	count(transactions_data_2.transaction_hash) AS n,
	'next_block'::text AS t_type
FROM
	transactions_data_2
WHERE
	transactions_data_2.blacklist = '{NULL}'::text[]
	AND (transactions_data_2.lowbasefee + transactions_data_2.lowtip + transactions_data_2.minertransaction + transactions_data_2.congested) = 0
	AND transactions_data_2.blocksdelay = 1
	AND transactions_data_2.mined > (CURRENT_DATE - '30 days'::interval)
WITH NO DATA;

CREATE MATERIALIZED VIEW operators_all AS  SELECT nested.operator_id,
    count(DISTINCT nested.pubkey) AS numberofvalidators,
    array_agg(DISTINCT nested.relays) AS relays
   FROM ( SELECT validator_pubkeys.pubkey,
            validator_pubkeys.operator_id,
            unnest(block_production.relays) AS relays
           FROM validator_pubkeys
             LEFT JOIN block_production ON block_production.proposer_pubkey::text = validator_pubkeys.pubkey::text) nested
  GROUP BY nested.operator_id
  WITH NO DATA;

