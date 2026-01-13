import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.connection import CassandraConnection
from src.config import Config
from src.queries import QueryExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_query_1_integrity(executor):
    result = executor.query_1_session_item_lookup(338, 4)
    assert result is not None
    assert result.artist == "Faithless"
    assert result.song_title == "Music Matters (Mark Knight Dub)"
    logger.info("Test Query 1: PASS")

def test_query_2_integrity(executor):
    results = executor.query_2_user_session_history(10, 182)
    assert len(results) == 4
    assert results[0].artist == "Down To The Bone"
    logger.info("Test Query 2: PASS")

def test_query_3_integrity(executor):
    results = executor.query_3_users_by_song('All Hands Against His Own')
    assert len(results) == 3
    user_names = [f"{r.user_first_name} {r.user_last_name}" for r in results]
    assert "Jacqueline Lynch" in user_names
    logger.info("Test Query 3: PASS")

def run_tests():
    conn = CassandraConnection()
    try:
        session = conn.connect(keyspace=Config.CASSANDRA_KEYSPACE)
        executor = QueryExecutor(session)
        
        test_query_1_integrity(executor)
        test_query_2_integrity(executor)
        test_query_3_integrity(executor)
        
        logger.info("All integrity tests passed successfully.")
    finally:
        conn.disconnect()

if __name__ == "__main__":
    run_tests()
