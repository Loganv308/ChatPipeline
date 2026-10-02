-- Test seed data: a couple of channels so Sync.py has something to push
-- into the collector's channel cache. Ids are the real Twitch user ids so
-- stream polling against the live Twitch API lines up.
INSERT INTO channels (id, name) VALUES
    (71092938, 'xqc'),
    (26490481, 'summit1g')
ON CONFLICT DO NOTHING;
