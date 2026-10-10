#!/usr/bin/env bash
set -eo pipefail

echo "==> Initializing LocalStack AWS Resources for ECOBrio..."

# Configure AWS CLI endpoint
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ACCESS_KEY_ID="test"
export AWS_SECRET_ACCESS_KEY="test"

LOCALSTACK_ENDPOINT="http://localhost:4566"

echo "==> Creating SQS Dead Letter Queue..."
awslocal sqs create-queue \
    --queue-name ecobrio-dlq \
    --attributes MessageRetentionPeriod=1209600

echo "==> Creating SQS Queues..."
awslocal sqs create-queue \
    --queue-name ecobrio-incidents \
    --attributes VisibilityTimeout=30,MessageRetentionPeriod=86400

awslocal sqs create-queue \
    --queue-name ecobrio-credits \
    --attributes VisibilityTimeout=30,MessageRetentionPeriod=86400

echo "==> Creating S3 Buckets..."
awslocal s3 mb s3://ecobrio-receipts
awslocal s3 mb s3://ecobrio-wal-snapshots

echo "==> LocalStack AWS Resources Initialized Successfully!"
