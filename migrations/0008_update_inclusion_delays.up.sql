DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_7d;
DROP MATERIALIZED VIEW IF EXISTS inclusion_delay_30d;

CREATE MATERIALIZED VIEW inclusion_delay_7d AS
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'optimal'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay = 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'likely_insufficient_balance'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.congested = 1
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 0
	AND transactions_data.blocksdelay <= 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'borderline'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay = 1
  AND transactions_data.blacklist = '{NULL}'::text[]
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 0
	AND transactions_data.blacklist <> '{NULL}'::text[]
	AND transactions_data.blocksdelay <= 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
	AND transactions_data.blocksdelay > 1
	AND transactions_data.blocksdelay <= 30
	AND transactions_data.blacklist = '{NULL}'::text[]
WITH NO DATA;

CREATE MATERIALIZED VIEW inclusion_delay_30d AS
SELECT
	0 AS avg_delay,
	0 AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'miner'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'optimal'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay = 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'low_base_fee'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'low_tip'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 0
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'likely_insufficient_balance'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'congested'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.congested = 1
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 0
	AND transactions_data.blocksdelay <= 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'borderline'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay = 1
  AND transactions_data.blacklist = '{NULL}'::text[]
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'ofac_delayed'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 0
	AND transactions_data.blacklist <> '{NULL}'::text[]
	AND transactions_data.blocksdelay <= 30
UNION
SELECT
	avg(transactions_data.delay) AS avg_delay,
	avg(transactions_data.blocksdelay) AS avg_block_delay,
	count(transactions_data.transaction_hash) AS n,
	'unknown'::text AS t_type
FROM
	transactions_data
WHERE
	transactions_data.minertransaction = 0
	AND transactions_data.congested = 0
	AND transactions_data.lowbasefee = 0
	AND transactions_data.lowtip = 0
	AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
	AND transactions_data.blocksdelay > 1
	AND transactions_data.blocksdelay <= 30
	AND transactions_data.blacklist = '{NULL}'::text[]
WITH NO DATA;
