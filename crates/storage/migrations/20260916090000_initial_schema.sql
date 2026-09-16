-- Initial market-data schema: the instruments we track, the trades that print
-- on them, and periodic level-2 book snapshots.

CREATE TYPE trade_side AS ENUM ('buy', 'sell');

-- One row per instrument per exchange. Everything else keys off `id` rather
-- than the ticker text, so a rename upstream does not rewrite history.
CREATE TABLE symbols (
    id                 integer     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange           text        NOT NULL,
    symbol             text        NOT NULL,
    base_asset         text        NOT NULL,
    quote_asset        text        NOT NULL,
    price_precision    smallint    NOT NULL,
    quantity_precision smallint    NOT NULL,
    active             boolean     NOT NULL DEFAULT true,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT symbols_exchange_symbol_key UNIQUE (exchange, symbol),
    CONSTRAINT symbols_text_not_blank CHECK (
        length(btrim(exchange)) > 0
        AND length(btrim(symbol)) > 0
        AND length(btrim(base_asset)) > 0
        AND length(btrim(quote_asset)) > 0
    ),
    CONSTRAINT symbols_precision_in_range CHECK (
        price_precision BETWEEN 0 AND 12
        AND quantity_precision BETWEEN 0 AND 12
    )
);

-- numeric(24, 12) is deliberate: 24 significant digits stay inside the 96-bit
-- mantissa of the Rust `Decimal` these columns are read into, so a round trip
-- through the service cannot silently lose precision. Money is never a float.
CREATE TABLE trades (
    id                bigint         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol_id         integer        NOT NULL REFERENCES symbols (id) ON DELETE CASCADE,
    exchange_trade_id bigint         NOT NULL,
    price             numeric(24, 12) NOT NULL,
    quantity          numeric(24, 12) NOT NULL,
    side              trade_side     NOT NULL,
    traded_at         timestamptz    NOT NULL,
    ingested_at       timestamptz    NOT NULL DEFAULT now(),

    CONSTRAINT trades_price_positive CHECK (price > 0),
    CONSTRAINT trades_quantity_positive CHECK (quantity > 0)
);

-- A feed replays trades after every reconnect. This index makes the exchange's
-- own id the idempotency key, so ingestion can INSERT ... ON CONFLICT DO
-- NOTHING and stop caring whether it has seen the batch before.
CREATE UNIQUE INDEX trades_dedupe_key ON trades (symbol_id, exchange_trade_id);

-- Serves the REST history query: newest-first within one symbol and window.
CREATE INDEX trades_symbol_traded_at_idx ON trades (symbol_id, traded_at DESC);

-- Append-only, roughly time-ordered data: a BRIN index costs a few pages
-- instead of gigabytes and is enough for the retention sweep, which scans the
-- whole table by timestamp rather than by symbol.
CREATE INDEX trades_traded_at_brin ON trades USING brin (traded_at) WITH (pages_per_range = 32);

-- Level-2 snapshots. `sequence` is the exchange's update id: it makes
-- snapshots ordered and, with the unique constraint, replay-safe.
CREATE TABLE book_snapshots (
    id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol_id   integer     NOT NULL REFERENCES symbols (id) ON DELETE CASCADE,
    sequence    bigint      NOT NULL,
    captured_at timestamptz NOT NULL,
    -- [{"price": "...", "quantity": "..."}] with decimals as strings, so the
    -- JSON layer cannot round-trip them through a float either.
    bids        jsonb       NOT NULL,
    asks        jsonb       NOT NULL,

    CONSTRAINT book_snapshots_sequence_key UNIQUE (symbol_id, sequence),
    CONSTRAINT book_snapshots_sequence_non_negative CHECK (sequence >= 0),
    CONSTRAINT book_snapshots_sides_are_arrays CHECK (
        jsonb_typeof(bids) = 'array' AND jsonb_typeof(asks) = 'array'
    )
);

CREATE INDEX book_snapshots_latest_idx ON book_snapshots (symbol_id, captured_at DESC);
