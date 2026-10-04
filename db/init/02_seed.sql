-- Test seed data: a couple of channels so Sync.py has something to push
-- into the collector's channel cache. twitch_id holds the real Twitch user
-- ids so stream polling against the live Twitch API lines up.
INSERT INTO channels (name, twitch_id) VALUES
    ('xqc', '71092938'),
    ('summit1g', '26490481')
ON CONFLICT DO NOTHING;
