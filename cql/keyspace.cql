CREATE KEYSPACE IF NOT EXISTS music_streaming 
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE music_streaming;

DESCRIBE KEYSPACE music_streaming;
