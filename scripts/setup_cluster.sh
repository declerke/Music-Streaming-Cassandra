#!/bin/bash

set -e

echo "=========================================="
echo "Cassandra Cluster Setup"
echo "=========================================="

if ! docker info > /dev/null 2>&1; then
    echo "✗ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✓ Docker is running"

echo "Starting Cassandra container..."
docker-compose up -d

echo "Waiting for Cassandra to start (60-90 seconds)..."
for i in {1..90}; do
    if docker logs cassandra-local 2>&1 | grep -q "Startup complete"; then
        echo "✓ Cassandra is ready!"
        break
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "Testing connection..."
docker exec cassandra-local cqlsh -e "SELECT release_version FROM system.local;" || {
    echo "✗ Connection test failed"
    exit 1
}

echo ""
echo "=========================================="
echo "✓ Cassandra cluster is ready!"
echo "=========================================="
echo ""
echo "Connection details:"
echo "  Host: 127.0.0.1"
echo "  Port: 9042"
echo ""
echo "Next steps:"
echo "  1. Download data: bash data/download_data.sh"
echo "  2. Run ETL: python scripts/run_etl.py"
