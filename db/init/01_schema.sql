-- ChatPipeline Postgres schema.
--
-- Mirrors what src/Sync.py reads and writes. Applied automatically by the
-- postgres image the first time docker-compose.test.yml starts with an
-- empty data volume (everything in /docker-entrypoint-initdb.d runs once,
-- in filename order).

-- id is Postgres's own surrogate key; streams and messages reference it.
-- twitch_id is the numeric Twitch user id, filled in by Sync.py from the
-- collector's channel cache (rows are matched by name). When SEED_CHANNELS
-- is empty and channels come from this table instead, rows need twitch_id
-- set or the collector won't pick them up.
CREATE TABLE IF NOT EXISTS channels (
    id          SERIAL      PRIMARY KEY,
    name        TEXT        NOT NULL UNIQUE,
    twitch_id   TEXT
);

-- Current state per stream, re-upserted by Sync.py every pass
-- (ON CONFLICT (id) DO UPDATE).
CREATE TABLE IF NOT EXISTS streams (
    id            TEXT        PRIMARY KEY,
    channel_id    BIGINT      NOT NULL REFERENCES channels (id),
    title         TEXT,
    game_name     TEXT,
    started_at    TIMESTAMPTZ,
    peak_viewers  INTEGER,
    is_live       BOOLEAN     NOT NULL DEFAULT TRUE
);

-- message_id is the dedup key for ON CONFLICT (message_id) DO NOTHING.
-- stream_id is NULL when the channel wasn't live; Sync.py upserts streams
-- before messages so this FK is satisfied.
CREATE TABLE IF NOT EXISTS messages (
    message_id  TEXT        PRIMARY KEY,
    channel_id    BIGINT      NOT NULL REFERENCES channels (id),
    -- channels.name copied in by Sync.py on insert; older rows are
    -- backfilled by Sanitizer.py.
    channel_name  TEXT,
    stream_id   TEXT        REFERENCES streams (id),
    user_id     TEXT,
    username    TEXT,
    message     TEXT,
    timestamp   TIMESTAMPTZ,
    subscriber  BOOLEAN,
    is_bot      BOOLEAN
);

CREATE INDEX IF NOT EXISTS idx_messages_channel_ts ON messages (channel_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_messages_stream     ON messages (stream_id);

-- Append-only log of messages the collector filtered out.
CREATE TABLE IF NOT EXISTS skipped_messages (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reason        TEXT,
    message_id    TEXT,
    channel_name  TEXT,
    username      TEXT,
    content       TEXT,
    raw_tags      TEXT,
    timestamp     TIMESTAMPTZ
);
