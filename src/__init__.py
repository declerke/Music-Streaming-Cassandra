# ========================================
# src/__init__.py
# ========================================
"""
Music Streaming Cassandra Analytics Package

A query-driven data modeling project demonstrating Apache Cassandra's
strengths for music streaming analytics with modern Python tooling.

Author: Your Name
Version: 1.0.0
"""

__version__ = '1.0.0'
__author__ = 'Your Name'
__all__ = [
    'Config',
    'CassandraConnection',
    'get_cassandra_session',
    'MusicStreamingETL',
    'QueryExecutor',
    'initialize_schema',
    'drop_schema',
]

from .config import Config
from .connection import CassandraConnection, get_cassandra_session
from .etl import MusicStreamingETL, run_etl_pipeline
from .queries import QueryExecutor, print_query_results
from .models import initialize_schema, drop_schema


# ========================================
# tests/__init__.py
# ========================================
"""
Test suite for Music Streaming Cassandra project.

Run tests with:
    pytest tests/ -v
    pytest tests/ --cov=src --cov-report=html
"""

__all__ = []