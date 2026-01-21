# S3 CSV Discovery Lambda

This Lambda function scans an S3 bucket for new or updated CSV files and enqueues them to an SQS queue for downstream processing. It is designed to be idempotent, fault-tolerant, and memory-efficient, with minimal in-memory state and predictable execution behavior.

The function tracks previously processed files using a JSON state file stored in S3, allowing it to detect new files or updates via ETag comparison while avoiding duplicate enqueues.

## High-Level Flow
1. Load processing state from S3
2. List CSV files under the configured S3 prefix
3. Detect new or updated files via ETag comparison
4. Mark files as queued in state
5. Enqueue file metadata to SQS
6. Persist updated state back to S3
7. Return execution summary

## Key Features
- **Deterministic S3 discovery**
    - Processes only files matching: prefix/<subfolder>/<file>.csv
    - Enforces exactly one folder level beneath the prefix
    - Skips configured folders (e.g. logs, temp, backups)
- **Idempotent processing**
    - Uses S3 object ETags to detect new or updated files
    - Prevents duplicate processing of unchanged files
- **S3-backed state tracking**
    - Stores processing state as JSON in S3
    - Tracks file status, size, folder, ETag, and queue timestamp
    - Avoids DynamoDB dependency for cost and operational simplicity
- **Resilient execution**
    - Continues processing after individual file failures
    - Returns HTTP 207 Multi-Status when partial failures occur
    - Logs structured execution metrics for observability
- **Low memory footprint**
    - Streams S3 listings via pagination
    - Avoids in-memory aggregation of file metadata

## Environment Variables

`S3_SOURCE_BUCKET`    Bucket containing source CSV files
`S3_PREFIX`	          Prefix to scan for CSVs
`SQS_QUEUE_URL`       Target SQS queue for downstream processing
`S3_STATE_BUCKET`     Bucket used to store processing state
`S3_STATE_KEY`        State file key (default: state/processed_files.json)

## Output
```json
{
  "statusCode": 200,
  "queued": 12,
  "skipped": 4,
  "errors": 0,
  "execution_time_seconds": 1.42
}
```