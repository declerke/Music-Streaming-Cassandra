import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;
"""

TABLE_SONGS_BY_SESSION_CQL = """
CREATE TABLE IF NOT EXISTS songs_by_session (
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song_title TEXT,
    song_length DECIMAL,
    PRIMARY KEY (session_id, item_in_session)
) WITH CLUSTERING ORDER BY (item_in_session ASC);
"""

INSERT_SONGS_BY_SESSION_CQL = """
INSERT INTO songs_by_session (
    session_id, item_in_session, artist, song_title, song_length
) VALUES (?, ?, ?, ?, ?);
"""

QUERY_SONGS_BY_SESSION_CQL = """
SELECT artist, song_title, song_length 
FROM songs_by_session 
WHERE session_id = ? AND item_in_session = ?;
"""

TABLE_SONGS_BY_USER_SESSION_CQL = """
CREATE TABLE IF NOT EXISTS songs_by_user_session (
    user_id INT,
    session_id INT,
    item_in_session INT,
    artist TEXT,
    song_title TEXT,
    user_first_name TEXT,
    user_last_name TEXT,
    PRIMARY KEY ((user_id, session_id), item_in_session)
) WITH CLUSTERING ORDER BY (item_in_session ASC);
"""

INSERT_SONGS_BY_USER_SESSION_CQL = """
INSERT INTO songs_by_user_session (
    user_id, session_id, item_in_session, 
    artist, song_title, user_first_name, user_last_name
) VALUES (?, ?, ?, ?, ?, ?, ?);
"""

QUERY_SONGS_BY_USER_SESSION_CQL = """
SELECT artist, song_title, item_in_session, 
       user_first_name, user_last_name
FROM songs_by_user_session 
WHERE user_id = ? AND session_id = ?;
"""

TABLE_USERS_BY_SONG_CQL = """
CREATE TABLE IF NOT EXISTS users_by_song (
    song_title TEXT,
    user_id INT,
    user_first_name TEXT,
    user_last_name TEXT,
    PRIMARY KEY (song_title, user_id)
) WITH CLUSTERING ORDER BY (user_id ASC);
"""

INSERT_USERS_BY_SONG_CQL = """
INSERT INTO users_by_song (
    song_title, user_id, user_first_name, user_last_name
) VALUES (?, ?, ?, ?);
"""

QUERY_USERS_BY_SONG_CQL = """
SELECT user_first_name, user_last_name 
FROM users_by_song 
WHERE song_title = ?;
"""

DROP_KEYSPACE = "DROP KEYSPACE IF EXISTS music_streaming;"

@dataclass
class SongBySession:
    session_id: int
    item_in_session: int
    artist: str
    song_title: str
    song_length: float

@dataclass
class SongByUserSession:
    user_id: int
    session_id: int
    item_in_session: int
    artist: str
    song_title: str
    user_first_name: str
    user_last_name: str

@dataclass
class UserBySong:
    song_title: str
    user_id: int
    user_first_name: str
    user_last_name: str

def get_all_table_create_statements():
    return [
        TABLE_SONGS_BY_SESSION_CQL,
        TABLE_SONGS_BY_USER_SESSION_CQL,
        TABLE_USERS_BY_SONG_CQL
    ]

def initialize_schema(session):
    try:
        session.execute(KEYSPACE_CQL)
        session.set_keyspace('music_streaming')
        for create_statement in get_all_table_create_statements():
            session.execute(create_statement)
        logger.info("Schema initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Schema initialization failed: {e}")
        return False

def drop_schema(session):
    try:
        session.execute(DROP_KEYSPACE)
        logger.info("Keyspace music_streaming dropped")
        return True
    except Exception as e:
        logger.error(f"Failed to drop schema: {e}")
        return False
