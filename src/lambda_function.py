import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Iterator, Dict, Any, Optional
import logging

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# AWS clients
s3 = None
sqs = None

# Environment variables
S3_SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
S3_STATE_BUCKET = os.getenv("S3_STATE_BUCKET")  # Can be same as source bucket
S3_STATE_KEY = os.getenv("S3_STATE_KEY", "state/processed_files.json")        

def get_s3_client():
    """Get or create S3 client."""
    global s3
    if s3 is None:
        s3 = boto3.client('s3')
    return s3


def get_sqs_client():
    """Get or create SQS client."""
    global sqs
    if sqs is None:
        sqs = boto3.client('sqs')
    return sqs                                     

def load_state() -> Dict[str, Dict[str, Any]]:
    """
    Load the state file from S3.
    
    Returns:
        Dict mapping s3_key -> {etag, status, queued_at, folder, size}
    """
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=S3_STATE_BUCKET, Key=S3_STATE_KEY)
        state = json.loads(response['Body'].read().decode('utf-8'))
        logger.info("📥 Loaded state: %d files tracked", len(state))
        return state
    except s3_client.exceptions.NoSuchKey:
        logger.info("📝 No existing state file, starting fresh")
        return {}
    except Exception as e:
        logger.error("❌ Failed to load state: %s", e)
        raise

def save_state(state: Dict[str, Dict[str, Any]]) -> None:
    """
    Save the state file to S3.
    
    Args:
        state: Dictionary mapping s3_key -> metadata
    """
    s3_client = get_s3_client()
    try:
        s3_client.put_object(
            Bucket=S3_STATE_BUCKET,
            Key=S3_STATE_KEY,
            Body=json.dumps(state, indent=2, default=str),
            ContentType='application/json'
        )
        logger.info("💾 Saved state: %d files tracked", len(state))
    except Exception as e:
        logger.error("❌ Failed to save state: %s", e)
        raise

def should_process_file(file_meta: Dict[str, Any], state: Dict[str, Dict[str, Any]]) -> bool:
    """
    Determines if a file should be processed based on S3 state tracking.
    Checks if file is new or has been updated (different etag).
    
    Args:
        file_meta (Dict[str, Any]): Metadata of the file to evaluate.
        state (Dict[str, Dict[str, Any]]): Current state of processed files.

    Returns:
        bool: True if the file should be processed, False otherwise.
    """
    try:
        s3_key = file_meta["key"]
        
        if s3_key not in state:
            logger.info("✨ New file detected: %s", s3_key)
            return True
        
        if state[s3_key].get("etag") != file_meta["etag"]:
            logger.info("🔄 Updated file detected: %s (old etag: %s, new etag: %s)", 
                    s3_key, state[s3_key].get("etag"), file_meta["etag"])
            return True
        
        return False  # No changes detected
    except ClientError as e:
        logger.error("❌ Error checking state for %s: %s", file_meta["key"], e)
        # Fail safe: if we can't check, don't process to avoid duplicates
        raise

def enqueue_to_sqs(file_meta: Dict[str, Any]) -> None:
    """
    Sends file metadata to SQS for processing by downstream workers.

    Args:
        file_meta (Dict[str, Any]): File metadata to enqueue.
    """
    message_body = {
        "bucket": file_meta["bucket"],
        "key": file_meta["key"],
        "etag": file_meta["etag"],
        "size": file_meta["size"],
        "folder": file_meta["folder"],
        "filename": file_meta["filename"],
        "queued_at": datetime.now(timezone.utc).isoformat(),
    }
    sqs_client = get_sqs_client()
    try:
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageAttributes={
                'folder': {
                    'StringValue': file_meta["folder"],
                    'DataType': 'String'
                },
                'etag': {
                    'StringValue': file_meta["etag"],
                    'DataType': 'String'
                }
            }
        )
        logger.info("📤 Enqueued to SQS: %s", file_meta["key"])
    except ClientError as e:
        logger.error("❌ Failed to enqueue %s: %s", file_meta["key"], e)
        raise

def mark_queued(file_meta: Dict[str, Any], state: Dict[str, Dict[str, Any]]) -> None:
    """
    Marks a file as queued in the state dictionary.

    Args:
        file_meta (Dict[str, Any]): File metadata to mark as queued.
        state (Dict[str, Dict[str, Any]]): Current state of processed files.
    """
    try:
        state[file_meta["key"]] = {
            "etag": file_meta["etag"],
            "status": "queued",
            "queued_at": datetime.now(timezone.utc).isoformat(),
            "folder": file_meta["folder"],
            "size": file_meta["size"],
        }
        logger.debug("✅ Marked as queued in DDB: %s", file_meta["key"])
    except ClientError as e:
        logger.error("❌ Failed to mark queued %s: %s", file_meta["key"], e)
        raise

def list_csv_files_per_subfolder(
        bucket_name: str,   
        prefix: str, 
        skip_folders: Optional[set[str]] = None,
        s3_client: Optional[boto3.client] = None,
    ) -> Iterator[Dict[str, Any]]:
    """
    Yields metadata for CSV files located exactly one level below `prefix`
    in the specified S3 bucket.

    Contract:
    - Only files matching: prefix/<folder>/<file>.csv are considered
    - Only one subfolder depth is allowed
    - Folders in `skip_folders` are ignored
    - No in-memory aggregation is performed

    Args:
        bucket_name (str): S3 bucket name
        prefix (str): S3 prefix to scan
        skip_folders (set[str] | None): Folder names to skip (case-insensitive)

    Yields:
        Dict[str, Any]: File-level metadata required for downstream processing
    """
    try: 
        s3_client = s3_client or get_s3_client()
        skip_folders = {s.lower() for s in (skip_folders or set())}
        paginator = s3_client.get_paginator("list_objects_v2")
        
        discovered = 0
        skipped_folders_seen: set[str] = set()
        
        logger.info("🔍 Scanning bucket=%s prefix=%s", bucket_name, prefix)

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                
                # Fast rejection: skip non-CSV files
                if not key.endswith(".csv"):
                    continue
                
                # Remove prefix and normalize
                relative = key[len(prefix):].lstrip("/")
                parts = relative.split("/")
                
                # Enforce exactly one subfolder
                if len(parts) != 2:
                    continue
                
                folder, filename = parts
                
                if folder.lower() in skip_folders:
                    skipped_folders_seen.add(folder)
                    continue
                
                discovered += 1

                yield {
                    "bucket": bucket_name,
                    "prefix": prefix,
                    "folder": folder,
                    "filename": filename,
                    "key": key,
                    "etag": obj.get("ETag", "").strip('"'),
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                }
        
        # Summary logging (bounded, deterministic)
        logger.info("🔍 Discovered %d CSV file(s)", discovered)

        if skipped_folders_seen:
            logger.info("⏩ Skipped folders: %s", ", ".join(sorted(skipped_folders_seen)))
    except ClientError as e:
        logger.error("❌ S3 error listing files in bucket=%s prefix=%s: %s", bucket_name, prefix, e)
        raise

def lambda_handler(event, context):
    """
    Discovery Lambda: Scans S3 for new/updated CSVs and enqueues them for processing.
    Uses S3 JSON file for state
    
    Note: DynamoDB could be used instead of S3 for state tracking for better scalability. But costs money.
    """
    # Early Guardrails for incorrect configurations
    required_vars = [S3_SOURCE_BUCKET, SQS_QUEUE_URL, S3_STATE_BUCKET]
    if not all(required_vars):
        logger.error("❌ Missing required environment variables: S3_SOURCE_BUCKET, SQS_QUEUE_URL, or S3_STATE_BUCKET")
        raise RuntimeError("Missing required environment variables")
    
    # Main execution
    execution_start = datetime.now(timezone.utc)
    queued_count = 0
    skipped_count = 0
    error_count = 0
    
    logger.info("🔍 Scanning for new or updated CSVs...")
    
    try:
        # Load current state
        state = load_state()
        
        # Scan for CSV files
        for file_meta in list_csv_files_per_subfolder(
                S3_SOURCE_BUCKET,
                S3_PREFIX,
                skip_folders={"logs", "temp", "db_backup", "ngs_handoff_robin"},
        ):
            try:
                if should_process_file(file_meta, state):
                    # Atomicity: mark queued, then enqueue
                    # if mark_queued fails, SQS won't be called
                    # if enqueue fails, dynamoDB will still show it as queued
                    mark_queued(file_meta, state)
                    enqueue_to_sqs(file_meta)
                    queued_count += 1
                else:
                    skipped_count += 1
            except Exception as e:
                logger.error("❌ Error processing file %s: %s", file_meta["key"], e)
                error_count += 1
                # Continue processing other files despite errors
                continue
        
        # Save updated state
        save_state(state)

    except Exception as e:
        logger.error("❌ Fatal error during discovery: %s", e)
        raise

    execution_time = (datetime.now(timezone.utc) - execution_start).total_seconds()
    
    logger.info("✅ Discovery complete: %d queued, %d skipped, %d errors in %.2fs", 
               queued_count, skipped_count, error_count, execution_time)
    
    return {
        "statusCode": 200 if error_count == 0 else 207,  # 207 = Multi-Status
        "queued": queued_count,
        "skipped": skipped_count,
        "errors": error_count,
        "execution_time_seconds": execution_time
    }