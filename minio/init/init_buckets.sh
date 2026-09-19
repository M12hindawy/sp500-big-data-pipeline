#!/bin/sh
set -e

MINIO_ENDPOINT=${MINIO_ENDPOINT:-"http://minio:9000"}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-"minioadmin"}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-"minioadmin"}
MINIO_BUCKET=${MINIO_BUCKET:-"stock-data"}

echo "Configuring MinIO client alias and waiting for connection..."
until mc alias set localminio "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; do
    echo "MinIO not ready yet. Retrying in 2 seconds..."
    sleep 2
done

echo "Creating bucket '${MINIO_BUCKET}' if missing..."
if ! mc ls "localminio/${MINIO_BUCKET}" > /dev/null 2>&1; then
    mc mb "localminio/${MINIO_BUCKET}"
    echo "Bucket '${MINIO_BUCKET}' created."
else
    echo "Bucket '${MINIO_BUCKET}' already exists."
fi

# Upload Bronze datasets if available in /data
if [ -f "/data/sp500_stocks.csv" ]; then
    echo "Uploading /data/sp500_stocks.csv to localminio/${MINIO_BUCKET}/bronze/stocks/..."
    mc cp "/data/sp500_stocks.csv" "localminio/${MINIO_BUCKET}/bronze/stocks/sp500_stocks.csv"
else
    echo "Warning: /data/sp500_stocks.csv not found in /data. Check DATASET_SETUP.md."
fi

if [ -f "/data/sp500_companies.csv" ]; then
    echo "Uploading /data/sp500_companies.csv to localminio/${MINIO_BUCKET}/bronze/company/..."
    mc cp "/data/sp500_companies.csv" "localminio/${MINIO_BUCKET}/bronze/company/sp500_companies.csv"
else
    echo "Warning: /data/sp500_companies.csv not found in /data. Check DATASET_SETUP.md."
fi

echo "Verifying bucket contents:"
mc ls "localminio/${MINIO_BUCKET}"
mc ls -r "localminio/${MINIO_BUCKET}/bronze/" || true

echo "MinIO initialization completed successfully."
