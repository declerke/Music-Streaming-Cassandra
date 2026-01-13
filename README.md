# 🎵 Music Streaming Data Modeling with Apache Cassandra

A high-performance, query-driven ETL pipeline and NoSQL data model designed to analyze music streaming logs. This project transforms raw event data into optimized Cassandra tables, enabling sub-millisecond lookups for user activity and listening history.

## 🏗️ Architecture & Engineering Decisions
In Cassandra, we model data based on the queries we need to perform, not the entities themselves. This project implements three specific business requirements:

* **Query-First Design**: Tables are designed to exactly match specific `SELECT` statements, eliminating the need for expensive joins.
* **Denormalization**: Data is intentionally duplicated across tables (e.g., `artist` and `song_title` appear in multiple tables) to ensure all data for a single query resides within one partition.

* **Composite Partition Keys**: Used in `songs_by_user_session` to group all activity for a specific user and session on a single node, ensuring query efficiency.
* **Clustering Columns**: Leveraged to provide automatic server-side sorting (e.g., sorting songs by their sequence within a session).


## 🛠️ Technical Stack
* **Language**: Python 3.12
* **Database**: Apache Cassandra 5.0 (Dockerized)
* **Libraries**: `cassandra-driver`, `gevent`, `pandas`, `argparse`
* **Infrastructure**: Docker Compose

## 🚀 Getting Started

### 1. Environment Setup
Ensure you have Docker and Python installed. Install dependencies:

```bash
pip install -r requirements.txt
```

### 2. Infrastructure
Launch the single-node Cassandra cluster:

```bash
docker-compose up -d
```

### 3. Execution (ETL Pipeline)
Run the pipeline to initialize the keyspace, create tables, and load the 6,820 events:

```bash
python scripts/run_etl.py --drop-tables
```

### 4. Analysis
Execute the business logic queries to verify the model:

```bash
python scripts/run_queries.py
```

## 🧪 Quality Assurance
Validation is performed at the schema and data levels:

```bash
python tests/test_queries.py
```
The test suite verifies **Data Integrity** (row counts) and **Query Accuracy** (ensuring specific session lookups return correct metadata).

## 📊 Data Modeling & Business Logic
The model answers three core business questions:

### 1. Session Item Lookup (`songs_by_session`)
* **Query**: Get artist, song title, and length for a specific `session_id` and `item_in_session`.
* **Partition Key**: `session_id` (distributes data).
* **Clustering Key**: `item_in_session` (sorts data).

### 2. User Session History (`songs_by_user_session`)
* **Query**: Get artist, song, and user name for a specific `user_id` and `session_id`.
* **Composite Key**: `(user_id, session_id)` (ensures all session data is co-located).
* **Sorting**: `item_in_session` ASC.

### 3. Users by Song (`users_by_song`)
* **Query**: Find every user who listened to a specific song.
* **Primary Key**: `(song_title, user_id)` (the `user_id` ensures uniqueness since multiple users listen to the same song).



## 📈 Performance & Metrics
* **Dataset Size**: 6,820 raw events.
* **Ingestion Rate**: ~225 records/sec (using prepared statements).
* **Storage Strategy**: SimpleStrategy with a Replication Factor of 1.

---

## 📦 requirements.txt
```text
cassandra-driver
gevent
pandas
python-dotenv
```
