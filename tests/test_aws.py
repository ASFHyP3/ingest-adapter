import pathlib

import pytest
from moto import mock_aws
from moto.core import patch_client

import aws


def test_md5_for_s3_file_browse(s3_bucket, gunw_data_path):
    browse_key = 'browse.png'
    browse = gunw_data_path / browse_key
    aws.S3_CLIENT.upload_file(str(browse), s3_bucket, browse_key)

    browse_md5 = 'e5094bda56e2316f2ec71d708cf1b4e6'
    assert aws.md5_for_s3_file(s3_bucket, browse_key) == browse_md5
    assert aws.md5_for_s3_file(s3_bucket, browse_key, chunk_size=512) == browse_md5


def test_md5_for_s3_file_metadata(s3_bucket, gunw_data_path):
    metadata_key = 'metadata.json'
    metadata = gunw_data_path / metadata_key
    aws.S3_CLIENT.upload_file(str(metadata), s3_bucket, metadata_key)

    metadata_md5 = '3b938e3797b8d5a90728ff64f7209752'
    assert aws.md5_for_s3_file(s3_bucket, metadata_key) == metadata_md5
    assert aws.md5_for_s3_file(s3_bucket, metadata_key, chunk_size=1024) == metadata_md5


@pytest.fixture
def gunw_data_path():
    return pathlib.Path(__file__).parent / 'data' / 'output_files' / 'gunw'


@pytest.fixture
def s3_bucket():
    with mock_aws():
        patch_client(aws.S3_CLIENT)

        bucket_name = 'myBucket'
        location = {'LocationConstraint': 'us-west-2'}

        aws.S3_CLIENT.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

        yield bucket_name
