import os
import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import Config
from src.connection import CassandraConnection
from src.models import initialize_schema, drop_schema
from src.etl import run_etl_pipeline

def setup_logging(log_level='INFO'):
    Config.LOG_DIR.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(Config.LOG_DIR / 'etl.log')
        ]
    )
    return logging.getLogger(__name__)

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Run ETL pipeline to load music streaming data into Cassandra',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--init-only', action='store_true')
    parser.add_argument('--drop-tables', action='store_true')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    
    return parser.parse_args()

def main():
    args = parse_arguments()
    logger = setup_logging(args.log_level)
    
    try:
        logger.info("=" * 70)
        logger.info("MUSIC STREAMING ETL PIPELINE")
        logger.info("=" * 70)
        
        Config.validate()
        
        conn = CassandraConnection()
        session = conn.connect()
        
        if not conn.test_connection():
            logger.error("Connection test failed")
            return 1
        
        if args.drop_tables:
            drop_schema(session)
        
        if not initialize_schema(session):
            logger.error("Schema initialization failed")
            return 1
        
        if args.init_only:
            conn.disconnect()
            return 0
        
        success = run_etl_pipeline(session)
        conn.disconnect()
        
        if success:
            logger.info("=" * 70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            return 0
        else:
            return 1
    
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    sys.exit(main())