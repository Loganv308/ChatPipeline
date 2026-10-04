-- ChatPipeline data health check. Read-only.
--
-- Each row is one check: PASS / FAIL, or INFO for numbers worth seeing
-- that have no fixed right answer. Run against the local copy:
--   docker exec -i chatpipeline-postgres-local psql -U postgres -d chatpipeline < db/checks/health_check.sql
-- Several checks scan the whole messages table, so on production expect
-- it to take a while; run it off-peak.

\pset pager off

WITH
bots AS (SELECT unnest(ARRAY['streamelements', 'nightbot', 'fossabot', 'moobot', 'streamlabs']) AS name),
live AS (SELECT id, channel_id, started_at FROM streams WHERE is_live),
recent_gaps AS (
    SELECT channel_id,
           timestamp - lag(timestamp) OVER (PARTITION BY channel_id ORDER BY timestamp) AS gap
    FROM messages
    WHERE timestamp > now() - interval '1 hour'
      AND channel_id IN (SELECT channel_id FROM live)),
busiest AS (
    SELECT c.name, count(*) AS n, max(g.gap) AS longest
    FROM recent_gaps g JOIN channels c ON c.id = g.channel_id
    GROUP BY c.name ORDER BY n DESC LIMIT 1),
checks (area, check_name, value, ok) AS (

    -- ── Ingestion ──
    SELECT 'ingest', 'messages inserted in last 10 min',
           count(*)::text, count(*) > 0
    FROM messages WHERE created_at > now() - interval '10 minutes'
UNION ALL
    SELECT 'ingest', 'newest message age',
           (now() - max(created_at))::text, now() - max(created_at) < interval '5 minutes'
    FROM messages
UNION ALL
    SELECT 'ingest', 'busiest live channel, longest silence in last hour',
           coalesce((SELECT name || ': ' || longest::text || ' over ' || n || ' msgs' FROM busiest), 'no live channels'),
           -- only judged for a busy chat (~1+ msg/s); slow chats have natural lulls
           (SELECT CASE WHEN n >= 3600 THEN longest < interval '30 seconds' END FROM busiest)

    -- ── Channels ──
UNION ALL
    SELECT 'channels', 'channels with recent messages but no twitch_id',
           count(*)::text, count(*) = 0
    FROM channels c
    WHERE c.twitch_id IS NULL
      AND EXISTS (SELECT 1 FROM messages m WHERE m.channel_id = c.id AND m.created_at > now() - interval '1 day')

    -- ── Referential integrity ──
UNION ALL
    SELECT 'integrity', 'messages pointing at a missing channel',
           count(*)::text, count(*) = 0
    FROM messages m WHERE NOT EXISTS (SELECT 1 FROM channels c WHERE c.id = m.channel_id)
UNION ALL
    SELECT 'integrity', 'messages pointing at a missing stream',
           count(*)::text, count(*) = 0
    FROM messages m WHERE m.stream_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM streams s WHERE s.id = m.stream_id)
UNION ALL
    SELECT 'integrity', 'messages with no channel_name',
           count(*)::text, count(*) = 0
    FROM messages WHERE channel_name IS NULL
UNION ALL
    SELECT 'integrity', 'messages whose channel_name is out of date',
           count(*)::text, count(*) = 0
    FROM messages m JOIN channels c ON c.id = m.channel_id WHERE m.channel_name IS DISTINCT FROM c.name

    -- ── Timestamps ──
UNION ALL
    SELECT 'timestamps', 'shifted (sent > 30 min after inserted)',
           count(*)::text, count(*) = 0
    FROM messages WHERE timestamp > created_at + interval '30 minutes'
UNION ALL
    SELECT 'timestamps', 'in the future',
           count(*)::text, count(*) = 0
    FROM messages WHERE timestamp > now() + interval '1 minute'
UNION ALL
    SELECT 'timestamps', 'missing',
           count(*)::text, count(*) = 0
    FROM messages WHERE timestamp IS NULL
UNION ALL
    SELECT 'timestamps', 'backfilled (inserted > 1 min after sent)',
           count(*)::text, NULL
    FROM messages WHERE created_at > timestamp + interval '1 minute'

    -- ── Text / users ──
UNION ALL
    SELECT 'text', 'messages with control characters',
           count(*)::text, count(*) = 0
    FROM messages WHERE message ~ E'[\\x01-\\x08\\x0b-\\x1f\\x7f]'
UNION ALL
    SELECT 'text', 'messages with untrimmed or repeated whitespace',
           count(*)::text, count(*) = 0
    FROM messages WHERE message ~ E'^\\s|\\s$|\\s\\s'
UNION ALL
    SELECT 'text', 'messages over 500 characters',
           count(*)::text, count(*) = 0
    FROM messages WHERE length(message) > 500
UNION ALL
    SELECT 'users', 'usernames not lowercase/trimmed/<=25 chars',
           count(*)::text, count(*) = 0
    FROM messages WHERE username <> left(lower(btrim(username)), 25)
UNION ALL
    SELECT 'users', 'is_bot disagreeing with the bot list',
           count(*)::text, count(*) = 0
    FROM messages m WHERE m.is_bot IS DISTINCT FROM (m.username IN (SELECT name FROM bots))

    -- ── Streams ──
UNION ALL
    SELECT 'streams', 'messages during a live stream left without stream_id (older than 10 min)',
           count(*)::text, count(*) = 0
    FROM messages m JOIN live l ON l.channel_id = m.channel_id
    WHERE m.stream_id IS NULL AND m.timestamp >= l.started_at
      AND m.created_at < now() - interval '10 minutes'
UNION ALL
    SELECT 'streams', 'messages with a stream_id',
           round(100.0 * count(stream_id) / greatest(count(*), 1), 1)::text || '%', NULL
    FROM messages

    -- ── Sanitizer ──
UNION ALL
    SELECT 'sanitizer', 'sweep phase',
           coalesce((SELECT phase || ' (v' || version || ')' FROM sanitizer_progress), 'not started'),
           coalesce((SELECT phase = 'incremental' FROM sanitizer_progress), false)
UNION ALL
    SELECT 'sanitizer', 'last completed run',
           coalesce((now() - max(finished_at))::text || ' ago', 'never'),
           coalesce(now() - max(finished_at) < interval '15 minutes', false)
    FROM sanitizer_runs WHERE status = 'completed'
UNION ALL
    SELECT 'sanitizer', 'failed runs in last 24h',
           count(*)::text, count(*) = 0
    FROM sanitizer_runs WHERE status = 'failed' AND started_at > now() - interval '1 day'
UNION ALL
    SELECT 'sanitizer', 'changes recorded (all time)',
           count(*)::text, NULL
    FROM sanitizer_changes

    -- ── Audit: the sanitizer never altered what was imported ──
    -- Re-checks every recorded change against the only kind of change its
    -- column may receive (the same rules as the guards in Sanitizer.py,
    -- applied independently here).
UNION ALL
    SELECT 'audit', 'recorded changes outside their allowed kind',
           count(*)::text, count(*) = 0
    FROM sanitizer_changes c LEFT JOIN messages m ON m.message_id = c.message_id
    WHERE NOT coalesce(CASE c.column_name
        WHEN 'message' THEN
             regexp_replace(c.old_value, E'[\\s\\x01-\\x1f\\x7f]', '', 'g')
               = regexp_replace(c.new_value, E'[\\s\\x01-\\x1f\\x7f]', '', 'g')
          OR (length(c.new_value) = 500
              AND starts_with(regexp_replace(c.old_value, E'[\\s\\x01-\\x1f\\x7f]', '', 'g'),
                              regexp_replace(c.new_value, E'[\\s\\x01-\\x1f\\x7f]', '', 'g')))
        WHEN 'username'  THEN c.new_value = left(lower(btrim(c.old_value)), 25)
        WHEN 'timestamp' THEN
             extract(epoch FROM c.old_value::timestamptz - c.new_value::timestamptz) BETWEEN 3600 AND 50400
         AND mod(extract(epoch FROM c.old_value::timestamptz - c.new_value::timestamptz)::numeric, 3600) = 0
        WHEN 'stream_id'    THEN c.old_value IS NULL
        WHEN 'channel_name' THEN c.new_value = (SELECT name FROM channels WHERE id = m.channel_id)
        WHEN 'is_bot'       THEN c.new_value IN ('true', 'false')
        ELSE false END, false)
UNION ALL
    -- Nothing has modified a repaired value since: each column's current
    -- value is still the one its most recent recorded change wrote.
    SELECT 'audit', 'repaired values changed since their recorded repair',
           count(*)::text, count(*) = 0
    FROM (SELECT DISTINCT ON (message_id, column_name) *
          FROM sanitizer_changes ORDER BY message_id, column_name, id DESC) c
    JOIN messages m ON m.message_id = c.message_id
    WHERE CASE c.column_name
        WHEN 'message'      THEN m.message      IS DISTINCT FROM c.new_value
        WHEN 'username'     THEN m.username     IS DISTINCT FROM c.new_value
        WHEN 'timestamp'    THEN m.timestamp    IS DISTINCT FROM c.new_value::timestamptz
        WHEN 'stream_id'    THEN m.stream_id    IS DISTINCT FROM c.new_value
        WHEN 'channel_name' THEN m.channel_name IS DISTINCT FROM c.new_value
        WHEN 'is_bot'       THEN m.is_bot::text IS DISTINCT FROM c.new_value
        END
)
SELECT area, check_name AS "check", value,
       CASE WHEN ok IS NULL THEN 'INFO' WHEN ok THEN 'PASS' ELSE 'FAIL' END AS result
FROM checks;
