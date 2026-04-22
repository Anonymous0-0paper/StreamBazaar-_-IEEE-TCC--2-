#!/bin/bash

# StreamBazaar Flink Job Submission Script
# Submits the StreamBazaar auction orchestrator job to the Flink cluster

set -e

JOBMANAGER_HOST=${1:-flink-jobmanager}
JOBMANAGER_PORT=${2:-8081}
JAR_PATH="/opt/flink/lib/flink-integration.jar"
MAIN_CLASS="streambazaar.flink.StreamBazaarJob"

echo "Submitting StreamBazaar job to Flink cluster at $JOBMANAGER_HOST:$JOBMANAGER_PORT"

# Wait for jobmanager to be ready
MAX_RETRIES=30
RETRY_COUNT=0
while true; do
    if curl -s "http://$JOBMANAGER_HOST:$JOBMANAGER_PORT/v1/overview" > /dev/null 2>&1; then
        echo "✓ Flink jobmanager is ready"
        break
    fi
    
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "✗ Failed to connect to Flink jobmanager after $MAX_RETRIES retries"
        exit 1
    fi
    
    echo "Waiting for Flink jobmanager to be ready (attempt $RETRY_COUNT/$MAX_RETRIES)..."
    sleep 2
done

# Check if JAR exists
if [ ! -f "$JAR_PATH" ]; then
    echo "✗ JAR file not found: $JAR_PATH"
    exit 1
fi

echo "JAR file found: $JAR_PATH"

# Submit the job
echo "Submitting job..."
RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/octet-stream" \
    --data-binary @"$JAR_PATH" \
    "http://$JOBMANAGER_HOST:$JOBMANAGER_PORT/v1/jars/upload")

echo "Response: $RESPONSE"

# Extract upload path
UPLOAD_PATH=$(echo "$RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4)

if [ -z "$UPLOAD_PATH" ]; then
    echo "✗ Failed to upload JAR"
    exit 1
fi

echo "JAR uploaded: $UPLOAD_PATH"

# Submit the job
echo "Running job with main class: $MAIN_CLASS"
RUN_RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d '{"entrypoint":"'$MAIN_CLASS'","parallelism":2}' \
    "http://$JOBMANAGER_HOST:$JOBMANAGER_PORT/v1/jars/${UPLOAD_PATH}/run")

echo "Run response: $RUN_RESPONSE"

JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*"' | cut -d'"' -f4)

if [ -z "$JOB_ID" ]; then
    echo "✗ Failed to get job ID from response"
    exit 1
fi

echo "✓ StreamBazaar job submitted successfully"
echo "Job ID: $JOB_ID"
echo ""
echo "Monitor the job at: http://$JOBMANAGER_HOST:$JOBMANAGER_PORT/#/jobs/$JOB_ID"
