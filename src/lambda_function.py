import os
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from typing import Iterator, Dict, Any, Optional
import logging

import polars as pl

from src import processors

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

CSV_DISPATCH = {
    "single-b-cell-carterra-data": processors.process_exis_sequence_uploads,
    "strongbox-sync-bucket": processors.process_carterra_uploads,
}

def lambda_handler(event, context):
    """
    Processing Lambda: Scans CSV Discovery SQS for CSVs in need of processing.
    """
    record = event["Records"][0]
    body = json.loads(record["body"])

    bucket = body["bucket"]
    key = body["key"]
    
    try:
        processor = CSV_DISPATCH[bucket]
    except KeyError:
        logger.error(f"No CSV processor registered for bucket={bucket}")
        raise ValueError(f"No CSV processor registered for bucket={bucket}")
    
    logger.info(
        "Dispatching CSV processor for bucket=%s key=%s",
        bucket, key
    )
    
    processor(bucket, key)
    
    return {
        "statusCode": 200
    }