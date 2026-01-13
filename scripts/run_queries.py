import sys
import argparse
import logging
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import Config
from src.connection import get_cassandra_session
from src.queries import QueryExecutor, print_query_results

def setup_logging(log_level='INFO'):
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    
    return logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Execute analytical queries against Cassandra',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--query',
        type=int,
        choices=[1, 2, 3],
        help='Run specific query only (1, 2, or 3)'
    )
    
    parser.add_argument(
        '--session-id',
        type=int,
        default=338,
        help='Session ID for Query 1'
    )
    
    parser.add_argument(
        '--item-in-session',
        type=int,
        default=4,
        help='Item in session for Query 1'
    )
    
    parser.add_argument(
        '--user-id',
        type=int,
        default=10,
        help='User ID for Query 2'
    )
    
    parser.add_argument(
        '--user-session-id',
        type=int,
        default=182,
        help='Session ID for Query 2'
    )
    
    parser.add_argument(
        '--song-title',
        type=str,
        default='All Hands Against His Own',
        help='Song title for Query 3'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level'
    )
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("=" * 70)
        logger.info("MUSIC STREAMING ANALYTICS QUERIES")
        logger.info("=" * 70)
        
        with get_cassandra_session(Config.CASSANDRA_KEYSPACE) as session:
            executor = QueryExecutor(session)
            
            if args.query == 1:
                result = executor.query_1_session_item_lookup(
                    args.session_id,
                    args.item_in_session
                )
                results = {'query_1': result, 'query_2': [], 'query_3': []}
            
            elif args.query == 2:
                result = executor.query_2_user_session_history(
                    args.user_id,
                    args.user_session_id
                )
                results = {'query_1': {}, 'query_2': result, 'query_3': []}
            
            elif args.query == 3:
                result = executor.query_3_users_by_song(args.song_title)
                results = {'query_1': {}, 'query_2': [], 'query_3': result}
            
            else:
                results = executor.run_all_queries()
            
            print_query_results(results)
            
            logger.info("\nQuery execution completed")
            return 0
    
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 130
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(main())