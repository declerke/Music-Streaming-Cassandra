USE music_streaming;

CREATE TABLE IF NOT EXISTS songs_by_session (
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song_title TEXT,
    song_length DECIMAL,
    PRIMARY KEY (session_id, item_in_session)
) WITH CLUSTERING ORDER BY (item_in_session ASC)
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS songs_by_user_session (
    user_id INT,
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song_title TEXT,
    user_first_name TEXT,
    user_last_name TEXT,
    PRIMARY KEY ((user_id, session_id), item_in_session)
) WITH CLUSTERING ORDER BY (item_in_session ASC)
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS users_by_song (
    song_title TEXT,
    user_id INT,
    user_first_name TEXT,
    user_last_name TEXT,
    PRIMARY KEY (song_title, user_id)
) WITH CLUSTERING ORDER BY (user_id ASC)
  AND compaction = {'class': 'SizeTieredCompactionStrategy'}
  AND compression = {'sstable_compression': 'LZ4Compressor'};

DESCRIBE TABLES;
