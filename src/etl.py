import csv
import logging
from datetime import datetime
from typing import Dict, List
from cassandra.cluster import Session
from cassandra.query import BatchStatement, ConsistencyLevel

from .config import Config
from .models import (
    INSERT_SONGS_BY_SESSION_CQL,
    INSERT_SONGS_BY_USER_SESSION_CQL,
    INSERT_USERS_BY_SONG_CQL
)

logger = logging.getLogger(__name__)

class MusicStreamingETL:
    def __init__(self, session: Session):
        self.session = session
        self.stats = {
            'rows_read': 0,
            'rows_inserted_table1': 0,
            'rows_inserted_table2': 0,
            'rows_inserted_table3': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        
        self.insert_songs_by_session = self.session.prepare(INSERT_SONGS_BY_SESSION_CQL)
        self.insert_songs_by_user_session = self.session.prepare(INSERT_SONGS_BY_USER_SESSION_CQL)
        self.insert_users_by_song = self.session.prepare(INSERT_USERS_BY_SONG_CQL)
    
    def read_csv(self) -> List[Dict]:
        logger.info(f"Reading CSV file: {Config.EVENT_LOG_FILE}")
        events = []
        
        try:
            with open(Config.EVENT_LOG_FILE, 'r', encoding='utf8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['artist']:
                        events.append(row)
                        self.stats['rows_read'] += 1
            
            logger.info(f"Read {len(events)} valid events from CSV")
            return events
        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {Config.EVENT_LOG_FILE}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
            raise
    
    def insert_into_songs_by_session(self, event: Dict):
        try:
            self.session.execute(
                self.insert_songs_by_session,
                (
                    int(event['sessionId']),
                    int(event['itemInSession']),
                    event['artist'],
                    event['song'],
                    float(event['length'])
                )
            )
            self.stats['rows_inserted_table1'] += 1
        except Exception as e:
            logger.error(f"Error inserting into songs_by_session: {e}")
            self.stats['errors'] += 1
    
    def insert_into_songs_by_user_session(self, event: Dict):
        try:
            self.session.execute(
                self.insert_songs_by_user_session,
                (
                    int(event['userId']),
                    int(event['sessionId']),
                    int(event['itemInSession']),
                    event['artist'],
                    event['song'],
                    event['firstName'],
                    event['lastName']
                )
            )
            self.stats['rows_inserted_table2'] += 1
        except Exception as e:
            logger.error(f"Error inserting into songs_by_user_session: {e}")
            self.stats['errors'] += 1
    
    def insert_into_users_by_song(self, event: Dict):
        try:
            self.session.execute(
                self.insert_users_by_song,
                (
                    event['song'],
                    int(event['userId']),
                    event['firstName'],
                    event['lastName']
                )
            )
            self.stats['rows_inserted_table3'] += 1
        except Exception as e:
            logger.error(f"Error inserting into users_by_song: {e}")
            self.stats['errors'] += 1
    
    def load_event(self, event: Dict):
        self.insert_into_songs_by_session(event)
        self.insert_into_songs_by_user_session(event)
        self.insert_into_users_by_song(event)
    
    def run(self):
        try:
            self.stats['start_time'] = datetime.now()
            
            logger.info("=" * 60)
            logger.info("STARTING ETL PIPELINE")
            logger.info("=" * 60)
            
            events = self.read_csv()
            
            logger.info(f"Loading {len(events)} events into Cassandra tables...")
            
            for i, event in enumerate(events, 1):
                self.load_event(event)
                
                if i % 1000 == 0:
                    logger.info(f"  Processed {i}/{len(events)} events...")
            
            self.stats['end_time'] = datetime.now()
            self.print_summary()
            
            logger.info("ETL pipeline completed successfully")
            return True
        
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}", exc_info=True)
            return False
    
    def print_summary(self):
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info("=" * 60)
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Execution Time: {duration:.2f} seconds")
        logger.info(f"CSV Rows Read: {self.stats['rows_read']}")
        logger.info("")
        logger.info("Records Inserted:")
        logger.info(f"  songs_by_session:       {self.stats['rows_inserted_table1']}")
        logger.info(f"  songs_by_user_session:  {self.stats['rows_inserted_table2']}")
        logger.info(f"  users_by_song:          {self.stats['rows_inserted_table3']}")
        logger.info(f"  Total:                  {sum([
            self.stats['rows_inserted_table1'],
            self.stats['rows_inserted_table2'],
            self.stats['rows_inserted_table3']
        ])}")
        logger.info("")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info("=" * 60)

def run_etl_pipeline(session: Session) -> bool:
    etl = MusicStreamingETL(session)
    return etl.run()