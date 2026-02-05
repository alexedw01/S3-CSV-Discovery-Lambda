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

def process_carterra_uploads(bucket:str, key:str) -> None:
    """
    Process the CSV assuming 

    Args:
        bucket (str): Name of the AWS S3 Bucket. 
        key (str): File Path of CSV located in the specificed bucket.
    """
    pass