from gevent import monkey
monkey.patch_all()

import logging
from contextlib import contextmanager
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from src.config import Config

logger = logging.getLogger(__name__)

class CassandraConnection:
    
    def __init__(self):
        self.cluster = None
        self.session = None
        self._connected = False
    
    def connect(self, keyspace=None):
        try:
            connection_params = Config.get_connection_params()
            
            logger.info(f"Connecting to Cassandra at {Config.CASSANDRA_HOST}:{Config.CASSANDRA_PORT}")
            self.cluster = Cluster(**connection_params)
            
            if keyspace:
                self.session = self.cluster.connect(keyspace)
                logger.info(f"Connected to keyspace: {keyspace}")
            else:
                self.session = self.cluster.connect()
                logger.info("Connected to Cassandra (no keyspace selected)")
            
            self._connected = True
            return self.session
        
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def disconnect(self):
        if self.session:
            self.session.shutdown()
            logger.info("Session closed")
        
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cluster connection closed")
        
        self._connected = False
    
    def test_connection(self):
        try:
            if not self._connected:
                self.connect()
            
            query = "SELECT release_version FROM system.local"
            row = self.session.execute(query).one()
            
            if row:
                logger.info(f"Connected to Cassandra {row.release_version}")
                return True
            else:
                logger.error("Connection test failed: No response from Cassandra")
                return False
        
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def execute_cql_file(self, cql_file_path):
        try:
            with open(cql_file_path, 'r') as f:
                cql_statements = f.read()
            
            statements = [s.strip() for s in cql_statements.split(';') if s.strip()]
            
            for statement in statements:
                logger.debug(f"Executing: {statement[:50]}...")
                self.session.execute(statement)
            
            logger.info(f"Executed {len(statements)} statements from {cql_file_path}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to execute CQL file: {e}")
            return False
    
    def keyspace_exists(self, keyspace_name):
        try:
            query = f"""
                SELECT keyspace_name 
                FROM system_schema.keyspaces 
                WHERE keyspace_name = '{keyspace_name}'
            """
            result = self.session.execute(query).one()
            return result is not None
        
        except Exception as e:
            logger.error(f"Error checking keyspace: {e}")
            return False
    
    def table_exists(self, keyspace_name, table_name):
        try:
            query = f"""
                SELECT table_name 
                FROM system_schema.tables 
                WHERE keyspace_name = '{keyspace_name}' 
                AND table_name = '{table_name}'
            """
            result = self.session.execute(query).one()
            return result is not None
        
        except Exception as e:
            logger.error(f"Error checking table: {e}")
            return False
    
    def get_table_count(self, table_name):
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            result = self.session.execute(query).one()
            return result[0] if result else 0
        
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

@contextmanager
def get_cassandra_session(keyspace=None):
    connection = CassandraConnection()
    try:
        session = connection.connect(keyspace=keyspace)
        yield session
    finally:
        connection.disconnect()