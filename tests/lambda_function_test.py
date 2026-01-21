import boto3
import pytest
import json
from moto import mock_aws
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from src import lambda_function

@pytest.fixture(autouse=True)
def moto_aws():
    with mock_aws():
        yield

@pytest.fixture(scope='function', autouse=True)
def aws_credentials(monkeypatch):
    """Mock AWS credentials for moto - applied automatically to all tests."""
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'testing')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-west-2')


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up environment variables for S3 state tracking."""
    monkeypatch.setenv("S3_SOURCE_BUCKET", "test-source-bucket")
    monkeypatch.setenv("S3_PREFIX", "input/")
    monkeypatch.setenv("SQS_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue")
    monkeypatch.setenv("S3_STATE_BUCKET", "test-state-bucket")
    monkeypatch.setenv("S3_STATE_KEY", "state/processed_files.json")


@pytest.fixture
def setup_aws_resources():
    """Create mock AWS resources."""
    # S3 - Source bucket
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-source-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # S3 - State bucket
    s3.create_bucket(
        Bucket="test-state-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # SQS
    sqs = boto3.client("sqs", region_name="us-west-2")
    queue = sqs.create_queue(QueueName="test-queue")
    
    return {
        "s3": s3,
        "sqs": sqs,
        "queue_url": queue["QueueUrl"]
    }


# ============================================================================
# Unit Tests: list_csv_files_per_subfolder
# ============================================================================


def test_list_csv_files_single_level():
    """Test that only single-level CSV files are discovered."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # Valid CSVs (single level)
    s3.put_object(Bucket="test-bucket", Key="input/folder1/a.csv", Body=b"1,2,3")
    s3.put_object(Bucket="test-bucket", Key="input/folder1/b.csv", Body=b"4,5,6")
    
    # Invalid files
    s3.put_object(Bucket="test-bucket", Key="input/folder1/nested/c.csv", Body=b"bad")  # Too deep
    s3.put_object(Bucket="test-bucket", Key="input/folder1/d.txt", Body=b"7,8,9")      # Not CSV
    s3.put_object(Bucket="test-bucket", Key="input/skipped/x.csv", Body=b"skip")       # Skipped folder
    
    results = list(
        lambda_function.list_csv_files_per_subfolder(
            bucket_name="test-bucket",
            prefix="input/",
            skip_folders={"skipped"},
            s3_client=s3,
        )
    )
    
    print(results)
    
    assert len(results) == 2
    assert all(r["folder"] == "folder1" for r in results)
    assert all(r["key"].endswith(".csv") for r in results)
    assert set(r["filename"] for r in results) == {"a.csv", "b.csv"}



def test_list_csv_files_empty_bucket():
    """Test handling of empty bucket."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    results = list(
        lambda_function.list_csv_files_per_subfolder(
            bucket_name="test-bucket",
            prefix="input/",
            s3_client=s3,
        )
    )
    
    print(results)
    
    assert len(results) == 0



def test_list_csv_files_skip_folders_case_insensitive():
    """Test that skip_folders is case-insensitive."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    s3.put_object(Bucket="test-bucket", Key="input/Logs/a.csv", Body=b"1")
    s3.put_object(Bucket="test-bucket", Key="input/TEMP/b.csv", Body=b"2")
    s3.put_object(Bucket="test-bucket", Key="input/valid/c.csv", Body=b"3")
    
    results = list(
        lambda_function.list_csv_files_per_subfolder(
            bucket_name="test-bucket",
            prefix="input/",
            skip_folders={"logs", "temp"},  # lowercase
            s3_client=s3,
        )
    )
    
    print(results)
    
    assert len(results) == 1
    assert results[0]["folder"] == "valid"


# ============================================================================
# Unit Tests: load_state and save_state
# ============================================================================


def test_load_state_no_existing_file(mock_env_vars):
    """Test load_state when no state file exists."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-state-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # Patch both the s3 client getter AND the environment variables
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"):
        
        state = lambda_function.load_state()
        assert state == {}



def test_load_state_existing_file(mock_env_vars):
    """Test load_state with existing state file."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-state-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # Create existing state
    existing_state = {
        "input/folder1/a.csv": {
            "etag": "abc123",
            "status": "queued",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 5,
        }
    }
    s3.put_object(
        Bucket="test-state-bucket",
        Key="state/processed_files.json",
        Body=json.dumps(existing_state)
    )
    
    # Patch both the s3 client getter AND the environment variables
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"):
    
        state = lambda_function.load_state()
        print(state)
        assert len(state) == 1
        assert "input/folder1/a.csv" in state
        assert state["input/folder1/a.csv"]["etag"] == "abc123"



def test_save_state(mock_env_vars):
    """Test save_state writes state file correctly."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-state-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    state = {
        "input/folder1/a.csv": {
            "etag": "abc123",
            "status": "queued",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 5,
        }
    }

    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"):
        lambda_function.save_state(state)
        
        # Verify state was saved
        response = s3.get_object(Bucket="test-state-bucket", Key="state/processed_files.json")
        saved_state = json.loads(response['Body'].read().decode('utf-8'))
        assert saved_state == state


# ============================================================================
# Unit Tests: should_process_file
# ============================================================================

def test_should_process_file_new_file():
    """Test that new files are flagged for processing."""
    state = {}
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "new.csv",
        "key": "input/folder1/new.csv",
        "etag": "abc123",
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    
    result = lambda_function.should_process_file(file_meta, state)
    assert result is True


def test_should_process_file_updated_file():
    """Test that updated files (different etag) are flagged for processing."""
    state = {
        "input/folder1/existing.csv": {
            "etag": "old_etag",
            "status": "queued",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 5,
        }
    }
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "existing.csv",
        "key": "input/folder1/existing.csv",
        "etag": "abc123", # Different!
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    
    result = lambda_function.should_process_file(file_meta, state)
    assert result is True


def test_should_process_file_unchanged_file():
    """Test that unchanged files are skipped."""
    state = {
        "input/folder1/existing.csv": {
            "etag": "abc123",
            "status": "queued",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 5,
        }
    }
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "existing.csv",
        "key": "input/folder1/existing.csv",
        "etag": "abc123", # Same etag
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    
    result = lambda_function.should_process_file(file_meta, state)
    assert result is False


# ============================================================================
# Unit Tests: mark_queued
# ============================================================================

def test_mark_queued_success():
    """Test successful marking of file as queued."""
    state = {}
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "existing.csv",
        "key": "input/folder1/existing.csv",
        "etag": "abc123", 
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    lambda_function.mark_queued(file_meta, state)
    
    # Verify item was added to state
    assert "input/folder1/existing.csv" in state
    assert state["input/folder1/existing.csv"]["etag"] == "abc123"
    assert state["input/folder1/existing.csv"]["status"] == "queued"
    assert state["input/folder1/existing.csv"]["folder"] == "folder1"
    assert "queued_at" in state["input/folder1/existing.csv"]

def test_mark_queued_overwrite():
    """Test that mark_queued overwrites existing state entry."""
    state = {
        "input/folder1/existing.csv": {
            "etag": "old_etag",
            "status": "completed",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 5,
        }
    }
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "existing.csv",
        "key": "input/folder1/existing.csv",
        "etag": "new_etag", 
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    lambda_function.mark_queued(file_meta, state)
    
    # Verify item was updated in state
    assert state["input/folder1/existing.csv"]["etag"] == "new_etag"
    assert state["input/folder1/existing.csv"]["status"] == "queued"
    assert "queued_at" in state["input/folder1/existing.csv"]

def test_mark_queued_multiple_files():
    """Test marking multiple files as queued."""
    state = {}
    files_meta = [
        {
            "bucket": "test-bucket",
            "prefix": "input/",
            "folder": "folder1",
            "filename": "file1.csv",
            "key": "input/folder1/file1.csv",
            "etag": "etag1", 
            "size": 5,
            "last_modified": "2026-01-19T10:00:00Z",
        },
        {
            "bucket": "test-bucket",
            "prefix": "input/",
            "folder": "folder2",
            "filename": "file2.csv",
            "key": "input/folder2/file2.csv",
            "etag": "etag2", 
            "size": 10,
            "last_modified": "2026-01-19T10:00:00Z",
        }
    ]
    
    for file_meta in files_meta:
        lambda_function.mark_queued(file_meta, state)
    
    # Verify both items were added to state
    assert len(state) == 2
    for file_meta in files_meta:
        key = file_meta["key"]
        assert key in state
        assert state[key]["etag"] == file_meta["etag"]
        assert state[key]["status"] == "queued"
        assert state[key]["folder"] == file_meta["folder"]
        assert "queued_at" in state[key]


def test_mark_queued_invalid_input():
    """Test mark_queued with invalid input."""
    state = {}
    file_meta = {
        # Missing 'key' and 'etag'
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "invalid.csv",
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    
    with pytest.raises(KeyError):
        lambda_function.mark_queued(file_meta, state)

def test_mark_queued_append_to_exisiting_state():
    """Test that mark_queued appends to existing state without overwriting other entries."""
    state = {
        "input/folder1/other.csv": {
            "etag": "other_etag",
            "status": "completed",
            "queued_at": "2026-01-19T10:00:00Z",
            "folder": "folder1",
            "size": 15,
        }
    }
    file_meta = {
        "bucket": "test-bucket",
        "prefix": "input/",
        "folder": "folder1",
        "filename": "new.csv",
        "key": "input/folder1/new.csv",
        "etag": "new_etag", 
        "size": 5,
        "last_modified": "2026-01-19T10:00:00Z",
    }
    lambda_function.mark_queued(file_meta, state)
    
    # Verify both items exist in state
    assert len(state) == 2
    assert "input/folder1/other.csv" in state
    assert "input/folder1/new.csv" in state
# ============================================================================
# Unit Tests: enqueue_to_sqs
# ============================================================================


def test_enqueue_to_sqs_success(setup_aws_resources, mock_env_vars):
    """Test successful enqueueing to SQS."""
    resources = setup_aws_resources
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    with patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url):
        
        file_meta = {
            "bucket": "test-bucket",
            "key": "input/folder1/test.csv",
            "etag": "abc123",
            "size": 1024,
            "folder": "folder1",
            "filename": "test.csv",
            "queued_at": "2026-01-19T10:00:00Z",
        }
        
        lambda_function.enqueue_to_sqs(file_meta)
        
        # Verify message was sent
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        assert "Messages" in messages
        
        message_body = json.loads(messages["Messages"][0]["Body"])
        assert message_body["key"] == "input/folder1/test.csv"
        assert message_body["etag"] == "abc123"
        assert "queued_at" in message_body


# ============================================================================
# Integration Tests: lambda_handler
# ============================================================================


def test_lambda_handler_new_files(setup_aws_resources, mock_env_vars):
    """Test lambda_handler with new files."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]

    # Add CSV files
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"data1")
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/b.csv", Body=b"data2")
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        result = lambda_function.lambda_handler({}, {})
        
        assert result["statusCode"] == 200
        assert result["queued"] == 2
        assert result["skipped"] == 0
        assert result["errors"] == 0
        
        # Verify state file was created
        response = s3.get_object(Bucket="test-state-bucket", Key="state/processed_files.json")
        state = json.loads(response['Body'].read().decode('utf-8'))
        assert len(state) == 2
        assert "input/folder1/a.csv" in state
        assert "input/folder1/b.csv" in state



def test_lambda_handler_skip_unchanged_files(setup_aws_resources, mock_env_vars):
    """Test lambda_handler skips unchanged files."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add CSV file
    response = s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"data1")
    etag = response["ETag"].strip('"')
    
    # Pre-create state file with existing file
    existing_state = {
        "input/folder1/a.csv": {
            "etag": etag,
            "status": "queued",
            "queued_at": "2026-01-19T10:00:00Z"
        }
    }
    s3.put_object(
        Bucket="test-state-bucket",
        Key="state/processed_files.json",
        Body=json.dumps(existing_state)
    )
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        result = lambda_function.lambda_handler({}, {})
        
        assert result["queued"] == 0
        assert result["skipped"] == 1



def test_lambda_handler_updated_file(setup_aws_resources, mock_env_vars):
    """Test lambda_handler processes updated files (changed etag)."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add CSV file
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"updated_data")
    
    # Pre-create state file with old etag
    existing_state = {
        "input/folder1/a.csv": {
            "etag": "old_etag_123",
            "status": "completed",
            "queued_at": "2026-01-19T10:00:00Z"
        }
    }
    s3.put_object(
        Bucket="test-state-bucket",
        Key="state/processed_files.json",
        Body=json.dumps(existing_state)
    )
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        result = lambda_function.lambda_handler({}, {})
        
        assert result["queued"] == 1  # File was updated, so it should be queued
        assert result["skipped"] == 0

def test_lambda_handler_missing_env_vars():
    """Test lambda_handler fails gracefully with missing env vars."""
    with pytest.raises(RuntimeError, match="Missing required environment variables"):
        lambda_function.lambda_handler({}, {})

def test_lambda_handler_skip_folders(setup_aws_resources, mock_env_vars):
    """Test lambda_handler respects skip_folders."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add files in skipped and non-skipped folders
    s3.put_object(Bucket="test-source-bucket", Key="input/logs/a.csv", Body=b"skip")
    s3.put_object(Bucket="test-source-bucket", Key="input/temp/b.csv", Body=b"skip")
    s3.put_object(Bucket="test-source-bucket", Key="input/valid/c.csv", Body=b"process")
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        result = lambda_function.lambda_handler({}, {})
        
        assert result["queued"] == 1  # Only valid/c.csv

def test_lambda_handler_partial_failure(setup_aws_resources, mock_env_vars):
    """Test lambda_handler continues after individual file failures."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add CSV files
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"data1")
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/b.csv", Body=b"data2")
    
    # Mock enqueue_to_sqs to fail on first file
    original_enqueue = lambda_function.enqueue_to_sqs
    call_count = 0
    
    def failing_enqueue(file_meta):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ClientError({"Error": {"Code": "500"}}, "SendMessage")
        return original_enqueue(file_meta)
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"), \
         patch.object(lambda_function, "enqueue_to_sqs", side_effect=failing_enqueue):
        
        result = lambda_function.lambda_handler({}, {})
        
        assert result["statusCode"] == 207  # Multi-status
        assert result["errors"] == 1
        # Second file should still be processed despite first failing



def test_lambda_handler_multiple_runs_idempotency(setup_aws_resources, mock_env_vars):
    """Test that running lambda multiple times is idempotent."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add CSV file
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"data1")
    
    with patch.object(lambda_function, "get_s3_client", return_value=s3), \
         patch.object(lambda_function, "get_sqs_client", return_value=sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        # First run
        result1 = lambda_function.lambda_handler({}, {})
        assert result1["queued"] == 1
        assert result1["skipped"] == 0
        
        # Second run - should skip
        result2 = lambda_function.lambda_handler({}, {})
        assert result2["queued"] == 0
        assert result2["skipped"] == 1
        
        # Third run - should still skip
        result3 = lambda_function.lambda_handler({}, {})
        assert result3["queued"] == 0
        assert result3["skipped"] == 1


# ============================================================================
# Edge Case Tests
# ============================================================================


def test_etag_stripping():
    """Test that ETags are properly stripped of quotes."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    s3.put_object(Bucket="test-bucket", Key="input/folder1/a.csv", Body=b"data")
    
    results = list(
        lambda_function.list_csv_files_per_subfolder(
            bucket_name="test-bucket",
            prefix="input/",
            s3_client=s3,
        )
    )
    
    # ETags from S3 come with quotes, should be stripped
    assert '"' not in results[0]["etag"]



def test_datetime_serialization(setup_aws_resources, mock_env_vars):
    """Test that datetime objects are properly serialized."""
    resources = setup_aws_resources
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    with patch.object(lambda_function, "sqs", sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url):
        
        file_meta = {
            "bucket": "test-bucket",
            "key": "input/folder1/test.csv",
            "etag": "abc123",
            "size": 1024,
            "folder": "folder1",
            "filename": "test.csv",
        }
        
        # Should not raise JSON serialization error
        lambda_function.enqueue_to_sqs(file_meta)
        
        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        message_body = json.loads(messages["Messages"][0]["Body"])
        
        # Verify queued_at is ISO format string
        datetime.fromisoformat(message_body["queued_at"])  # Should not raise



def test_state_file_json_format(setup_aws_resources, mock_env_vars):
    """Test that state file is properly formatted JSON."""
    resources = setup_aws_resources
    s3 = resources["s3"]
    sqs = resources["sqs"]
    queue_url = resources["queue_url"]
    
    # Add CSV file
    s3.put_object(Bucket="test-source-bucket", Key="input/folder1/a.csv", Body=b"data1")
    
    with patch.object(lambda_function, "s3", s3), \
         patch.object(lambda_function, "sqs", sqs), \
         patch.object(lambda_function, "SQS_QUEUE_URL", queue_url), \
         patch.object(lambda_function, "S3_SOURCE_BUCKET", "test-source-bucket"), \
         patch.object(lambda_function, "S3_STATE_BUCKET", "test-state-bucket"), \
         patch.object(lambda_function, "S3_STATE_KEY", "state/processed_files.json"), \
         patch.object(lambda_function, "S3_PREFIX", "input/"):
        
        lambda_function.lambda_handler({}, {})
        
        # Verify state file is valid JSON
        response = s3.get_object(Bucket="test-state-bucket", Key="state/processed_files.json")
        state_content = response['Body'].read().decode('utf-8')
        
        # Should not raise
        state = json.loads(state_content)
        
        # Verify content type
        assert response['ContentType'] == 'application/json'



def test_empty_prefix_handling():
    """Test handling of empty or root prefix."""
    s3 = boto3.client("s3", region_name="us-west-2")
    s3.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
    )
    
    # Add file at root level
    s3.put_object(Bucket="test-bucket", Key="folder1/a.csv", Body=b"data")
    
    results = list(
        lambda_function.list_csv_files_per_subfolder(
            bucket_name="test-bucket",
            prefix="",  # Empty prefix
            s3_client=s3,
        )
    )
    
    assert len(results) == 1
    assert results[0]["folder"] == "folder1"
    assert results[0]["filename"] == "a.csv"