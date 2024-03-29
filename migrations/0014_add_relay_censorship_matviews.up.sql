CREATE MATERIALIZED VIEW relay_censorship_7d AS
WITH
relay_total_blocks AS (
    SELECT
        UNNEST(block_production.relays) AS relay_id,
        COUNT(DISTINCT(block_production.block_number)) AS total_blocks
    FROM
        block_production
    LEFT JOIN
        blocks
        ON blocks.block_number = block_production.block_number
    WHERE
        blocks.timestamp > (CURRENT_DATE - '7 days'::interval)
    GROUP BY
        relay_id
),
relay_uncensored_blocks AS (
    SELECT
        UNNEST(block_production.relays) AS relay_id,
        COUNT(DISTINCT(transactions_data.block_number)) AS uncensored_blocks
    FROM
        transactions_data
    LEFT JOIN
        blocks
        ON blocks.block_number = transactions_data.block_number
    LEFT JOIN
        block_production
        ON block_production.block_number = transactions_data.block_number
        AND blocks.block_hash = block_production.block_hash
    WHERE
        transactions_data.blacklist != '{NULL}'::text[]
        AND transactions_data.mined > (CURRENT_DATE - '7 days'::interval)
    GROUP BY
        relay_id
)

SELECT
    total.relay_id,
    total.total_blocks,
    COALESCE(uncensored.uncensored_blocks, 0) as uncensored_blocks
FROM
    relay_total_blocks total
    LEFT JOIN relay_uncensored_blocks uncensored
    ON total.relay_id = uncensored.relay_id

WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS relay_censorship_7d_relay_id ON relay_censorship_7d (relay_id);


CREATE MATERIALIZED VIEW relay_censorship_30d AS
WITH
relay_total_blocks AS (
    SELECT
        UNNEST(block_production.relays) AS relay_id,
        COUNT(DISTINCT(block_production.block_number)) AS total_blocks
    FROM
        block_production
    LEFT JOIN
        blocks
        ON blocks.block_number = block_production.block_number
    WHERE
        blocks.timestamp > (CURRENT_DATE - '30 days'::interval)
    GROUP BY
        relay_id
),
relay_uncensored_blocks AS (
    SELECT
        UNNEST(block_production.relays) AS relay_id,
        COUNT(DISTINCT(transactions_data.block_number)) AS uncensored_blocks
    FROM
        transactions_data
    LEFT JOIN
        blocks
        ON blocks.block_number = transactions_data.block_number
    LEFT JOIN
        block_production
        ON block_production.block_number = transactions_data.block_number
        AND blocks.block_hash = block_production.block_hash
    WHERE
        transactions_data.blacklist != '{NULL}'::text[]
        AND transactions_data.mined > (CURRENT_DATE - '30 days'::interval)
    GROUP BY
        relay_id
)

SELECT
    total.relay_id,
    total.total_blocks,
    COALESCE(uncensored.uncensored_blocks, 0) as uncensored_blocks
FROM
    relay_total_blocks total
    LEFT JOIN relay_uncensored_blocks uncensored
    ON total.relay_id = uncensored.relay_id

WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS relay_censorship_30d_relay_id ON relay_censorship_30d (relay_id);
