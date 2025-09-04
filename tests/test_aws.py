import pytest
from botocore.stub import Stubber

import aws


@pytest.fixture(autouse=True)
def sqs_stubber():
    with Stubber(aws.SQS_CLIENT) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def test_publish_message(sqs_stubber):
    sqs_stubber.add_response(
        method='send_message',
        expected_params={
            'QueueUrl': 'myQueue',
            'MessageBody': '{"identifier": "foo"}',
        },
        service_response={},
    )
    sqs_stubber.add_response(
        method='send_message',
        expected_params={
            'QueueUrl': 'myQueue',
            'MessageBody': '{"identifier": "bar"}',
        },
        service_response={},
    )

    aws.send_ingest_message(
        queue_url='myQueue',
        message={'identifier': 'foo'}  # type: ignore[typeddict-item]
    )

    aws.send_ingest_message(
        queue_url='myQueue',
        message={'identifier': 'bar'}  # type: ignore[typeddict-item]
    )


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
