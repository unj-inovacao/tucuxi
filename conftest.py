import boto3
import pytest
from moto import mock_s3
from moto import mock_sqs


@pytest.fixture
def objs():
    return [
        {"key": "TEST/teste_object.json", "data": "TEST DATA"},
        {"key": "TEST/teste_object2.json", "data": "TEST DATA 2"},
    ]


@pytest.fixture(scope="session")
def s3_name(request):
    bucket_name = "test.bucket"
    mock = mock_s3()
    mock.start()
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_name)
    request.addfinalizer(mock.stop)
    return bucket_name


@pytest.fixture(scope="session")
def sqs_url(request):
    queue_name = "test_queue"
    mock = mock_sqs()
    mock.start()
    conn = boto3.resource("sqs", region_name="us-east-1")
    queue_url = conn.create_queue(QueueName=queue_name).url
    request.addfinalizer(mock.stop)
    return queue_url
