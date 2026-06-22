import pathlib
import os

import pytest
from moto import mock_aws
from moto.core import patch_client


def pytest_configure(config):
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
    os.environ['AWS_REGION'] = 'us-west-2'


@pytest.fixture
def gunw_data_path():
    return pathlib.Path(__file__).parent / 'data' / 'output_files' / 'gunw'


@pytest.fixture
def s3_bucket():
    import aws

    with mock_aws():
        patch_client(aws.S3_CLIENT)

        bucket_name = 'myBucket'
        location = {'LocationConstraint': 'us-west-2'}

        aws.S3_CLIENT.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

        yield bucket_name
