import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Config:
    CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', '127.0.0.1')
    CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', '9042'))
    CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'music_streaming')
    
    CASSANDRA_PROTOCOL_VERSION = 4
    CASSANDRA_COMPRESSION = True
    CONNECTION_TIMEOUT = int(os.getenv('CONNECTION_TIMEOUT', '30'))
    
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / 'data'
    EVENT_LOG_FILE = DATA_DIR / os.getenv('EVENT_LOG_FILE', 'event_datafile_new.csv')
    
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
    CONCURRENT_REQUESTS = int(os.getenv('CONCURRENT_REQUESTS', '10'))
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_DIR = PROJECT_ROOT / 'logs'
    
    TABLE_SONGS_BY_SESSION = 'songs_by_session'
    TABLE_SONGS_BY_USER_SESSION = 'songs_by_user_session'
    TABLE_USERS_BY_SONG = 'users_by_song'
    
    @classmethod
    def validate(cls):
        cls.DATA_DIR.mkdir(exist_ok=True)
        cls.LOG_DIR.mkdir(exist_ok=True)
        
        if not cls.EVENT_LOG_FILE.exists():
            return False
        
        return True
    
    @classmethod
    def get_connection_params(cls):
        return {
            'contact_points': [cls.CASSANDRA_HOST],
            'port': cls.CASSANDRA_PORT,
            'protocol_version': cls.CASSANDRA_PROTOCOL_VERSION,
            'compression': cls.CASSANDRA_COMPRESSION,
            'connect_timeout': cls.CONNECTION_TIMEOUT
        }