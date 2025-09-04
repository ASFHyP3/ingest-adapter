import datetime
from unittest.mock import MagicMock, patch

import pytest

import aws
import gunw
from gunw import A19_URL, GUNW_USERNAME, TIBET_URL


def test_granule_ur_pattern():
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    expected = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*'
    assert gunw._granule_ur_pattern(granule_ur) == expected

    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    expected = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*'
    assert gunw._granule_ur_pattern(granule_ur) == expected


def test_generate_ingest_message(s3_bucket, gunw_data_path, monkeypatch):
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'metadata.json'), s3_bucket, 'myPrefix/myFilename.json')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'browse.png'), s3_bucket, 'myPrefix/myFilename.png')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'data.nc'), s3_bucket, 'myPrefix/myFilename.nc')

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456, tzinfo=datetime.UTC)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    job = {
        'job_id': 'myPrefix',
        'files': [
            {
                's3': {
                    'bucket': s3_bucket,
                    'key': 'myPrefix/myFilename.nc',
                },
            },
        ],
    }
    product = {
        'name': 'myFilename',
        'files': [
            {
                'name': 'myFilename.json',
                'type': 'metadata',
                'uri': 's3://myBucket/myPrefix/myFilename.json',
                'size': 2675,
                'checksum': '3b938e3797b8d5a90728ff64f7209752',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.nc',
                'type': 'data',
                'uri': 's3://myBucket/myPrefix/myFilename.nc',
                'size': 0,
                'checksum': 'd41d8cd98f00b204e9800998ecf8427e',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.png',
                'type': 'browse',
                'uri': 's3://myBucket/myPrefix/myFilename.png',
                'size': 737869,
                'checksum': 'e5094bda56e2316f2ec71d708cf1b4e6',
                'checksumType': 'md5',
            },
        ],
        'dataVersion': '1.0',
    }
    expected = {
        'identifier': 'myFilename',
        'collection': 'ARIA_S1_GUNW',
        'version': '1.6.1',
        'submissionTime': '2025-02-18T01:02:03.000456Z',
        'product': product,
        'provider': 'ASF_HyP3',
        'trace': 'ASF-TOOLS',
    }
    print(gunw._generate_ingest_message(job))
    assert gunw._generate_ingest_message(job) == expected


def test_process_job_if_not_archived(monkeypatch, s3_bucket, gunw_data_path):
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('GUNW_QUEUE_URL', 'myQueueUrl')

    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'metadata.json'), s3_bucket, 'myPrefix/myFilename.json')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'browse.png'), s3_bucket, 'myPrefix/myFilename.png')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'data.nc'), s3_bucket, 'myPrefix/myFilename.nc')

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456, tzinfo=datetime.UTC)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    job = {
        'job_id': 'myPrefix',
        'job_type': 'ARIA_S1_GUNW',
        'files': [
            {
                's3': {
                    'bucket': s3_bucket,
                    'key': 'myPrefix/myFilename.nc',
                },
            },
        ],
    }
    product = {
        'name': 'myFilename',
        'files': [
            {
                'name': 'myFilename.json',
                'type': 'metadata',
                'uri': 's3://myBucket/myPrefix/myFilename.json',
                'size': 2675,
                'checksum': '3b938e3797b8d5a90728ff64f7209752',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.nc',
                'type': 'data',
                'uri': 's3://myBucket/myPrefix/myFilename.nc',
                'size': 0,
                'checksum': 'd41d8cd98f00b204e9800998ecf8427e',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.png',
                'type': 'browse',
                'uri': 's3://myBucket/myPrefix/myFilename.png',
                'size': 737869,
                'checksum': 'e5094bda56e2316f2ec71d708cf1b4e6',
                'checksumType': 'md5',
            },
        ],
        'dataVersion': '1.0',
    }
    expected_ingest_message = {
        'identifier': 'myFilename',
        'collection': 'ARIA_S1_GUNW',
        'version': '1.6.1',
        'submissionTime': '2025-02-18T01:02:03.000456Z',
        'product': product,
        'provider': 'ASF_HyP3',
        'trace': 'ASF-TOOLS',
    }

    with (
        patch('util.exists_in_cmr', return_value=False) as mock_exists_in_cmr,
        patch('aws.send_ingest_message') as mock_publish_message,
    ):
        gunw.process_job(job, 'https://foo.com')

        mock_exists_in_cmr.assert_called_once_with(
            'cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', 'myFilename', gunw._granule_ur_pattern
        )
        mock_publish_message.assert_called_once_with('myQueueUrl', expected_ingest_message)

    with (
        patch('util.exists_in_cmr', return_value=True) as mock_exists_in_cmr,
        patch('aws.send_ingest_message') as mock_publish_message,
    ):
        gunw.process_job(job, 'https://foo.com')

        mock_exists_in_cmr.assert_called_once_with(
            'cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', 'myFilename', gunw._granule_ur_pattern
        )
        mock_publish_message.assert_not_called()


@pytest.mark.parametrize(
    'job_type,user_id,hyp3_url,expected_to_qualify',
    [
        ('ARIA_S1_GUNW', 'test-user', 'https://foo.com', True),
        ('INSAR_ISCE', GUNW_USERNAME, A19_URL, True),
        ('INSAR_ISCE', GUNW_USERNAME, TIBET_URL, True),
        ('INSAR_ISCE', 'test-user', A19_URL, False),
        ('INSAR_ISCE', GUNW_USERNAME, 'https://foo.com', False),
        ('ARIA_RAIDER', GUNW_USERNAME, A19_URL, True),
        ('ARIA_RAIDER', 'test-user', A19_URL, False),
        ('ARIA_RAIDER', GUNW_USERNAME, TIBET_URL, False),
        ('ARIA_RAIDER', GUNW_USERNAME, 'https://foo.com', False),
    ],
)
def test_process_job_if_qualifies(
    s3_bucket, gunw_data_path, monkeypatch, job_type: str, user_id: str, hyp3_url: str, expected_to_qualify: bool
):
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('GUNW_QUEUE_URL', 'myQueueUrl')

    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'metadata.json'), s3_bucket, 'myPrefix/myFilename.json')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'browse.png'), s3_bucket, 'myPrefix/myFilename.png')
    aws.S3_CLIENT.upload_file(str(gunw_data_path / 'data.nc'), s3_bucket, 'myPrefix/myFilename.nc')

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456, tzinfo=datetime.UTC)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    job = {
        'job_id': 'myPrefix',
        'job_type': job_type,
        'user_id': user_id,
        'files': [
            {
                's3': {
                    'bucket': s3_bucket,
                    'key': 'myPrefix/myFilename.nc',
                },
            },
        ],
    }
    product = {
        'name': 'myFilename',
        'files': [
            {
                'name': 'myFilename.json',
                'type': 'metadata',
                'uri': 's3://myBucket/myPrefix/myFilename.json',
                'size': 2675,
                'checksum': '3b938e3797b8d5a90728ff64f7209752',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.nc',
                'type': 'data',
                'uri': 's3://myBucket/myPrefix/myFilename.nc',
                'size': 0,
                'checksum': 'd41d8cd98f00b204e9800998ecf8427e',
                'checksumType': 'md5',
            },
            {
                'name': 'myFilename.png',
                'type': 'browse',
                'uri': 's3://myBucket/myPrefix/myFilename.png',
                'size': 737869,
                'checksum': 'e5094bda56e2316f2ec71d708cf1b4e6',
                'checksumType': 'md5',
            },
        ],
        'dataVersion': '1.0',
    }
    expected_ingest_message = {
        'identifier': 'myFilename',
        'collection': 'ARIA_S1_GUNW',
        'version': '1.6.1',
        'submissionTime': '2025-02-18T01:02:03.000456Z',
        'product': product,
        'provider': 'ASF_HyP3',
        'trace': 'ASF-TOOLS',
    }

    with (
        patch('util.exists_in_cmr', return_value=False) as mock_exists_in_cmr,
        patch('aws.send_ingest_message') as mock_publish_message,
    ):
        gunw.process_job(job, hyp3_url)

        if expected_to_qualify:
            mock_exists_in_cmr.assert_called_once_with(
                'cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', 'myFilename', gunw._granule_ur_pattern
            )
            mock_publish_message.assert_called_once_with('myQueueUrl', expected_ingest_message)
        else:
            mock_exists_in_cmr.assert_not_called()
            mock_publish_message.assert_not_called()


def test_gunw_get_file_type():
    assert gunw._get_file_type('foo.nc') == 'data'
    assert gunw._get_file_type('world.json') == 'metadata'
    assert gunw._get_file_type('browse.png') == 'browse'
    with pytest.raises(ValueError):
        assert gunw._get_file_type('bad_file.zip')
