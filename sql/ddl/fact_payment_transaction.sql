-- Fact: 1 row per transaction. CDC-enabled (status flips overwrite in place via MERGE).
CREATE TABLE IF NOT EXISTS gold.fact_payment_transaction (
    transaction_id  STRING        NOT NULL,
    game_key        STRING        NOT NULL,
    date_key        INT           NOT NULL,
    currency_key    STRING        NOT NULL,
    price           DECIMAL(18,4) NOT NULL,
    price_eur       DECIMAL(18,4) NOT NULL,
    status          STRING        NOT NULL,
    payment_date    DATE          NOT NULL,
    ingest_ts       TIMESTAMP     NOT NULL
)
USING DELTA
PARTITIONED BY (payment_date)
TBLPROPERTIES (delta.enableChangeDataFeed = true);
