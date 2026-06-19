-- SCD2: game metadata (genre, studio) can change over time.
CREATE TABLE IF NOT EXISTS gold.dim_game (
    game_sk     BIGINT GENERATED ALWAYS AS IDENTITY,
    game_key    STRING    NOT NULL,
    game_name   STRING    NOT NULL,
    genre       STRING,
    studio      STRING,
    valid_from  TIMESTAMP NOT NULL,
    valid_to    TIMESTAMP,
    is_current  BOOLEAN   NOT NULL
)
USING DELTA;
