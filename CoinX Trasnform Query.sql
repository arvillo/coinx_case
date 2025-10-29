----- Transform the data type and move it to staging schema
CREATE TABLE staging.users AS
SELECT 
	user_id AS user_id,
	region AS region,
	signup_date::timestamp AS signup_date
FROM raw_kyc.raw_users;

CREATE TABLE staging.tokens AS
SELECT 
	token_id AS token_id,
	token_name AS token_name,
	category AS category
FROM raw_tokens.raw_tokens;

CREATE TABLE staging.p2p_transfer AS 
SELECT 
	transfer_id AS transfer_id,
	sender_id AS sender_id,
	receiver_id AS receiver_id,
	token_id AS token_id,
	amount::float AS amount,
	status AS status,
	transfer_created_time::timestamp AS transfer_created_time,
	transfer_updated_time::timestamp AS transfer_updated_time
FROM raw_transactions.raw_p2p_transfers;

CREATE TABLE staging.trade AS
SELECT
	trade_id AS trade_id,
	user_id AS user_id,
	token_id AS token_id,
	side AS side,
	price_usd::float AS price_usd,
	quantity::float AS quantity,
	status AS status,
	trade_created_time::timestamp AS trade_created_time,
	trade_updated_time::timestamp AS trade_updated_time
FROM raw_transactions.raw_trades;


----- Change it to the analytics diagram

--- Generate Date Dim
CREATE TABLE analytics.date_dim (
    date_key      TEXT PRIMARY KEY,
    full_date     DATE NOT NULL,
    day_of_week   TEXT NOT NULL,
    day_number    INT NOT NULL,        
    month_number  INT NOT NULL,
    month_name    TEXT NOT NULL,
    year          INT NOT NULL,
    quarter       TEXT NOT NULL,
    is_weekend    TEXT NOT NULL
);

INSERT INTO analytics.date_dim (
    date_key,
    full_date,
    day_of_week,
    day_number,
    month_number,
    month_name,
    year,
    quarter,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD') AS date_key,
    d AS full_date,
    TRIM(TO_CHAR(d, 'Day')) AS day_of_week,
    EXTRACT(DAY FROM d) AS day_number,
    EXTRACT(MONTH FROM d) AS month_number,
    TRIM(TO_CHAR(d, 'Month')) AS month_name,
    EXTRACT(YEAR FROM d) AS year,
    'Q' || EXTRACT(QUARTER FROM d)::CHAR(1) AS quarter,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN 'Yes' ELSE 'No' END AS is_weekend
FROM generate_series(
    '2024-01-01'::DATE,
    '2025-12-31'::DATE,
    '1 day'::INTERVAL
) g(d);

--- Generate Time Dim
CREATE TABLE analytics.time_dim (
    time_key     TEXT PRIMARY KEY,
    full_time    TIME NOT NULL,
    hour         INT NOT NULL,
    minute       INT NOT NULL,
    second       INT NOT NULL,
    hour_12      INT NOT NULL,
    am_pm        TEXT NOT NULL
);

INSERT INTO analytics.time_dim (
    time_key,
    full_time,
    hour,
    minute,
    second,
    hour_12,
    am_pm
)
SELECT
    TO_CHAR(t, 'HH24MISS') AS time_key,
    t::TIME AS full_time,
    EXTRACT(HOUR FROM t) AS hour,
    EXTRACT(MINUTE FROM t) AS minute,
    EXTRACT(SECOND FROM t) AS second,
    TO_CHAR(t, 'HH12')::INT AS hour_12,
    TO_CHAR(t, 'AM') AS am_pm
FROM generate_series(
    '2000-01-01 00:00:00'::TIMESTAMP,
    '2000-01-01 23:59:59'::TIMESTAMP,
    '1 second'::INTERVAL
) g(t);

--- Populate the user dim based on staging data
CREATE TABLE analytics.user_dim (
    user_key   SERIAL PRIMARY KEY,
    user_id    TEXT UNIQUE NOT NULL,
    region      TEXT,
    signup_date DATE,
    first_transfer_date DATE,
    first_trade_date    DATE
);

WITH first_transfer AS (
    SELECT
        sender_id AS user_id,
        MIN(transfer_created_time::TIMESTAMP) AS first_transfer
    FROM
        staging.p2p_transfer
    GROUP BY
        sender_id
),
first_trade AS (
    SELECT
        user_id,
        MIN(trade_created_time::TIMESTAMP) AS first_trade
    FROM
        staging.trade
    GROUP BY
        user_id
)
INSERT INTO analytics.user_dim (
    user_id,
    region,
    signup_date,
    first_transfer_date,
    first_trade_date
)
SELECT
    u.user_id,
    u.region,
    u.signup_date::DATE,
    ftr.first_transfer,
    ftd.first_trade
FROM
    staging.users u
LEFT JOIN
    first_transfer ftr ON u.user_id = ftr.user_id
LEFT JOIN
    first_trade ftd ON u.user_id = ftd.user_id;

--- Populate the token dim
CREATE TABLE analytics.token_dim (
    token_key   SERIAL PRIMARY KEY,
    token_id    TEXT UNIQUE NOT NULL,
    token_name  TEXT,
    category TEXT
);

INSERT INTO analytics.token_dim (
    token_id,
    token_name,
    category
)
SELECT
    t.token_id,
    t.token_name,
    category
FROM
    staging.tokens t;

--- Generate the transfer data
CREATE TABLE analytics.p2p_transfer_fact (
    -- Surrogate Key (new PK)
    transfer_key SERIAL PRIMARY KEY,
    sender_key   INT NOT NULL,
    receiver_key INT NOT NULL,
    token_key         INT NOT NULL,
    transfer_date_key TEXT NOT NULL,
    transfer_time_key TEXT NOT NULL,
    transfer_id TEXT NOT NULL, 
	amount NUMERIC,
    status TEXT,
    CONSTRAINT fk_date     FOREIGN KEY (transfer_date_key) REFERENCES analytics.date_dim (date_key),
    CONSTRAINT fk_time     FOREIGN KEY (transfer_time_key) REFERENCES analytics.time_dim (time_key),
    CONSTRAINT fk_token    FOREIGN KEY (token_key)         REFERENCES analytics.token_dim (token_key),
    CONSTRAINT fk_sender   FOREIGN KEY (sender_key)   REFERENCES analytics.user_dim (user_key),
    CONSTRAINT fk_receiver FOREIGN KEY (receiver_key) REFERENCES analytics.user_dim (user_key)
);

-- Create a unique index on the original transfer_id to prevent duplicates
CREATE UNIQUE INDEX idx_p2p_transfer_fact_id ON analytics.p2p_transfer_fact (transfer_id);


-- Make sure the data is not duplicate when inserted
INSERT INTO analytics.p2p_transfer_fact (
	sender_key,
	receiver_key,
	token_key,
	transfer_date_key,
	transfer_time_key,
	transfer_id,
	amount,
	status
)
WITH p2p_transfer_not_duplicate AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY sender_id, receiver_id, token_id, amount, status ORDER BY transfer_created_time ASC) AS rn
	FROM staging.p2p_transfer
)
SELECT 
	ud.user_key AS sender_key,
	ud2.user_key AS receiver_key,
	td.token_key AS token_key,
	dd.date_key AS transfer_date_key,
	td2.time_key AS transfer_time_key,
	pt.transfer_id AS transfer_id,
	pt.amount AS amount,
	pt.status AS status
FROM
	p2p_transfer_not_duplicate pt LEFT JOIN analytics.user_dim ud ON pt.sender_id = ud.user_id
	LEFT JOIN analytics.user_dim ud2 ON pt.receiver_id = ud2.user_id
	LEFT JOIN analytics.token_dim td ON pt.token_id = td.token_id
	LEFT JOIN analytics.date_dim dd ON pt.transfer_created_time::DATE = dd.full_date
	LEFT JOIN analytics.time_dim td2 ON pt.transfer_created_time::TIME = td2.full_time
WHERE pt.rn = 1
;

--- Generate the trade data
CREATE TABLE analytics.trades_fact (
    -- Surrogate Key (new PK)
    trade_key SERIAL PRIMARY KEY,
    user_key   INT NOT NULL,
    token_key         INT NOT NULL,
    trade_date_key TEXT NOT NULL,
    trade_time_key TEXT NOT NULL,
    trade_id TEXT NOT NULL, 
	side TEXT,
	price_usd FLOAT,
	quantity FLOAT,
    status TEXT,
    CONSTRAINT fk_date     FOREIGN KEY (trade_date_key) REFERENCES analytics.date_dim (date_key),
    CONSTRAINT fk_time     FOREIGN KEY (trade_time_key) REFERENCES analytics.time_dim (time_key),
    CONSTRAINT fk_token    FOREIGN KEY (token_key)         REFERENCES analytics.token_dim (token_key),
    CONSTRAINT fk_sender   FOREIGN KEY (user_key)   REFERENCES analytics.user_dim (user_key)
);

-- Create a unique index on the original trade_id to prevent duplicates
CREATE UNIQUE INDEX idx_trades_fact_id ON analytics.trades_fact (trade_id);


-- Make sure the data is not duplicate when inserted

INSERT INTO analytics.trades_fact (
	user_key,
	token_key,
	trade_date_key,
	trade_time_key,
	trade_id,
	side,
	price_usd,
	quantity,
	status
)
WITH trade_not_duplicate AS (
	SELECT *, ROW_NUMBER() OVER (PARTITION BY trade_id, user_id, token_id, price_usd, quantity, status ORDER BY trade_created_time ASC) AS rn
	FROM staging.trade
)
SELECT 
	ud.user_key AS sender_key,
	td.token_key AS token_key,
	dd.date_key AS trade_date_key,
	td2.time_key AS trade_time_key,
	t.trade_id AS trade_id,
	t.side AS side,
	t.price_usd AS price_usd,
	t.quantity AS quantity,
	t.status AS status
FROM
	trade_not_duplicate t LEFT JOIN analytics.user_dim ud ON t.user_id = ud.user_id
	LEFT JOIN analytics.token_dim td ON t.token_id = td.token_id
	LEFT JOIN analytics.date_dim dd ON t.trade_created_time::DATE = dd.full_date
	LEFT JOIN analytics.time_dim td2 ON t.trade_created_time::TIME = td2.full_time
WHERE t.rn = 1
;