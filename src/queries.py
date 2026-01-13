"""
Query execution module for music streaming analytics.
Contains functions to execute the three main business queries.
"""
import logging
from typing import List, Dict, Any
from cassandra.cluster import Session

from .models import (
    QUERY_SONGS_BY_SESSION_CQL,
    QUERY_SONGS_BY_USER_SESSION_CQL,
    QUERY_USERS_BY_SONG_CQL
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Executes analytical queries against Cassandra."""
    
    def __init__(self, session: Session):
        self.session = session
        
        # Prepare statements for better performance
        self.query_songs_by_session = self.session.prepare(QUERY_SONGS_BY_SESSION_CQL)
        self.query_songs_by_user_session = self.session.prepare(QUERY_SONGS_BY_USER_SESSION_CQL)
        self.query_users_by_song = self.session.prepare(QUERY_USERS_BY_SONG_CQL)
    
    def query_1_session_item_lookup(self, session_id: int, item_in_session: int) -> Dict[str, Any]:
        """
        Query 1: Get song details for a specific session and item number.
        
        Business Question:
        "Give me the details of the song that was heard during 
         sessionId = 338, itemInSession = 4"
        
        Args:
            session_id: Session identifier
            item_in_session: Item number within session
        
        Returns:
            Dictionary with artist, song_title, song_length
        """
        logger.info(f"Query 1: session_id={session_id}, item_in_session={item_in_session}")
        
        try:
            result = self.session.execute(
                self.query_songs_by_session,
                (session_id, item_in_session)
            ).one()
            
            if result:
                data = {
                    'artist': result.artist,
                    'song_title': result.song_title,
                    'song_length': float(result.song_length)
                }
                logger.info(f"✓ Found: {data['artist']} - {data['song_title']}")
                return data
            else:
                logger.warning("✗ No results found")
                return {}
        
        except Exception as e:
            logger.error(f"✗ Query failed: {e}")
            return {}
    
    def query_2_user_session_history(self, user_id: int, session_id: int) -> List[Dict[str, Any]]:
        """
        Query 2: Get all songs played by a user in a specific session.
        
        Business Question:
        "Give me only the following: name of artist, song (sorted by itemInSession) 
         and user (first and last name) for userId = 10, sessionId = 182"
        
        Args:
            user_id: User identifier
            session_id: Session identifier
        
        Returns:
            List of dictionaries with artist, song_title, item_in_session, 
            user_first_name, user_last_name
        """
        logger.info(f"Query 2: user_id={user_id}, session_id={session_id}")
        
        try:
            results = self.session.execute(
                self.query_songs_by_user_session,
                (user_id, session_id)
            )
            
            data = []
            for row in results:
                data.append({
                    'artist': row.artist,
                    'song_title': row.song_title,
                    'item_in_session': row.item_in_session,
                    'user_first_name': row.user_first_name,
                    'user_last_name': row.user_last_name
                })
            
            if data:
                logger.info(f"✓ Found {len(data)} songs for user {data[0]['user_first_name']} {data[0]['user_last_name']}")
            else:
                logger.warning("✗ No results found")
            
            return data
        
        except Exception as e:
            logger.error(f"✗ Query failed: {e}")
            return []
    
    def query_3_users_by_song(self, song_title: str) -> List[Dict[str, str]]:
        """
        Query 3: Get all users who listened to a specific song.
        
        Business Question:
        "Give me every user name (first and last) in my music app history 
         who listened to the song 'All Hands Against His Own'"
        
        Args:
            song_title: Title of the song
        
        Returns:
            List of dictionaries with user_first_name, user_last_name
        """
        logger.info(f"Query 3: song_title='{song_title}'")
        
        try:
            results = self.session.execute(
                self.query_users_by_song,
                (song_title,)
            )
            
            data = []
            for row in results:
                data.append({
                    'user_first_name': row.user_first_name,
                    'user_last_name': row.user_last_name
                })
            
            if data:
                logger.info(f"✓ Found {len(data)} users who listened to '{song_title}'")
            else:
                logger.warning(f"✗ No users found for song '{song_title}'")
            
            return data
        
        except Exception as e:
            logger.error(f"✗ Query failed: {e}")
            return []
    
    def run_all_queries(self):
        """
        Run all three business queries with expected test values.
        
        Returns:
            Dictionary with results from all three queries
        """
        logger.info("=" * 60)
        logger.info("EXECUTING ALL BUSINESS QUERIES")
        logger.info("=" * 60)
        
        results = {}
        
        # Query 1: sessionId = 338, itemInSession = 4
        logger.info("\n[QUERY 1] Session Item Lookup")
        logger.info("-" * 60)
        results['query_1'] = self.query_1_session_item_lookup(338, 4)
        
        # Query 2: userId = 10, sessionId = 182
        logger.info("\n[QUERY 2] User Session History")
        logger.info("-" * 60)
        results['query_2'] = self.query_2_user_session_history(10, 182)
        
        # Query 3: song = 'All Hands Against His Own'
        logger.info("\n[QUERY 3] Users by Song")
        logger.info("-" * 60)
        results['query_3'] = self.query_3_users_by_song('All Hands Against His Own')
        
        logger.info("\n" + "=" * 60)
        logger.info("ALL QUERIES COMPLETED")
        logger.info("=" * 60)
        
        return results


def print_query_results(results: Dict):
    """
    Pretty print query results.
    
    Args:
        results: Dictionary containing query results
    """
    print("\n" + "=" * 60)
    print("QUERY RESULTS")
    print("=" * 60)
    
    # Query 1 Results
    print("\n[Query 1] Session 338, Item 4:")
    print("-" * 60)
    if results['query_1']:
        q1 = results['query_1']
        print(f"Artist:      {q1['artist']}")
        print(f"Song:        {q1['song_title']}")
        print(f"Length:      {q1['song_length']:.4f} seconds")
    else:
        print("No results found")
    
    # Query 2 Results
    print("\n[Query 2] User 10, Session 182:")
    print("-" * 60)
    if results['query_2']:
        user = results['query_2'][0]
        print(f"User: {user['user_first_name']} {user['user_last_name']}")
        print(f"\nSongs played ({len(results['query_2'])} total):")
        for song in results['query_2']:
            print(f"  [{song['item_in_session']}] {song['artist']} - {song['song_title']}")
    else:
        print("No results found")
    
    # Query 3 Results
    print("\n[Query 3] Song: 'All Hands Against His Own':")
    print("-" * 60)
    if results['query_3']:
        print(f"Users who listened ({len(results['query_3'])} total):")
        for user in results['query_3']:
            print(f"  • {user['user_first_name']} {user['user_last_name']}")
    else:
        print("No results found")
    
    print("\n" + "=" * 60)