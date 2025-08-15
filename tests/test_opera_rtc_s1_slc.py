import datetime
from unittest.mock import MagicMock, call, patch

import pytest
from botocore.stub import Stubber

import opera_rtc_s1_slc


@pytest.fixture(autouse=True)
def s3_stubber():
    with Stubber(opera_rtc_s1_slc.s3) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


@pytest.fixture(autouse=True)
def sqs_stubber():
    with Stubber(opera_rtc_s1_slc.sqs) as stubber:
        yield stubber
        stubber.assert_no_pending_responses()


def test_granule_ur_pattern():
    payload = 'OPERA_L2_RTC-S1_T075-160101-IW2_20250813T204041Z_20250813T235131Z_S1A_30_v1.0'
    output = 'OPERA_L2_RTC-S1_T075-160101-IW2_20250813T204041Z_*Z_S1A_30_v1.0'
    assert opera_rtc_s1_slc._granule_ur_pattern(payload) == output

    payload = 'OPERA_L2_RTC-S1_T169-362724-IW3_20230714T075543Z_20250209T102633Z_S1A_30_v1.0'
    output = 'OPERA_L2_RTC-S1_T169-362724-IW3_20230714T075543Z_*Z_S1A_30_v1.0'
    assert opera_rtc_s1_slc._granule_ur_pattern(payload) == output

    payload = 'OPERA_L2_RTC-S1_T154-329511-IW1_20211202T062842Z_20250703T003157Z_S1A_30_v1.0'
    output = 'OPERA_L2_RTC-S1_T154-329511-IW1_20211202T062842Z_*Z_S1A_30_v1.0'
    assert opera_rtc_s1_slc._granule_ur_pattern(payload) == output


def test_get_products(s3_stubber):
    s3_stubber.add_response(
        method='list_objects_v2',
        expected_params={
            'Bucket': 'myBucket',
            'Prefix': 'myJobId',
        },
        service_response={
            'Contents': [
                {'ETag': '"907a"', 'Size': 10, 'Key': 'myJobId/product.catalog.json'},
                {'ETag': '"a5a2"', 'Size': 11, 'Key': 'myJobId/product.log'},
                {'ETag': '"d552"', 'Size': 12, 'Key': 'myJobId/product1.h5'},
                {'ETag': '"f152"', 'Size': 13, 'Key': 'myJobId/product1.iso.xml'},
                {'ETag': '"725d"', 'Size': 14, 'Key': 'myJobId/product1_BROWSE.png'},
                {'ETag': '"64c1"', 'Size': 15, 'Key': 'myJobId/product1_VV.tif'},
                {'ETag': '"9e70"', 'Size': 16, 'Key': 'myJobId/product1_mask.tif'},
                {'ETag': '"e772"', 'Size': 17, 'Key': 'myJobId/product2.h5'},
                {'ETag': '"b6b8"', 'Size': 18, 'Key': 'myJobId/product2.iso.xml'},
                {'ETag': '"662f"', 'Size': 19, 'Key': 'myJobId/product2_BROWSE.png'},
                {'ETag': '"68ad"', 'Size': 20, 'Key': 'myJobId/product2_VV.tif'},
                {'ETag': '"fc90"', 'Size': 21, 'Key': 'myJobId/product2_mask.tif'},
            ]
        },
    )

    response = opera_rtc_s1_slc._get_products('myBucket', 'myJobId')
    assert sorted(response, key=lambda x: x['name']) == [
        {
            'name': 'product1',
            'files': [
                {
                    'name': 'product1.h5',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product1.h5',
                    'size': 12,
                    'checksum': 'd552',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product1.iso.xml',
                    'type': 'metadata',
                    'uri': 's3://myBucket/myJobId/product1.iso.xml',
                    'size': 13,
                    'checksum': 'f152',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product1_BROWSE.png',
                    'type': 'browse',
                    'uri': 's3://myBucket/myJobId/product1_BROWSE.png',
                    'size': 14,
                    'checksum': '725d',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product1_VV.tif',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product1_VV.tif',
                    'size': 15,
                    'checksum': '64c1',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product1_mask.tif',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product1_mask.tif',
                    'size': 16,
                    'checksum': '9e70',
                    'checksumType': 'md5',
                },
            ],
            'dataVersion': '1.0',
        },
        {
            'name': 'product2',
            'files': [
                {
                    'name': 'product2.h5',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product2.h5',
                    'size': 17,
                    'checksum': 'e772',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product2.iso.xml',
                    'type': 'metadata',
                    'uri': 's3://myBucket/myJobId/product2.iso.xml',
                    'size': 18,
                    'checksum': 'b6b8',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product2_BROWSE.png',
                    'type': 'browse',
                    'uri': 's3://myBucket/myJobId/product2_BROWSE.png',
                    'size': 19,
                    'checksum': '662f',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product2_VV.tif',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product2_VV.tif',
                    'size': 20,
                    'checksum': '68ad',
                    'checksumType': 'md5',
                },
                {
                    'name': 'product2_mask.tif',
                    'type': 'data',
                    'uri': 's3://myBucket/myJobId/product2_mask.tif',
                    'size': 21,
                    'checksum': 'fc90',
                    'checksumType': 'md5',
                },
            ],
            'dataVersion': '1.0',
        },
    ]


def test_get_file_type():
    assert opera_rtc_s1_slc._get_file_type('foo.tif') == 'data'
    assert opera_rtc_s1_slc._get_file_type('bar.h5') == 'data'
    assert opera_rtc_s1_slc._get_file_type('hello/world.iso.xml') == 'metadata'
    assert opera_rtc_s1_slc._get_file_type('browse.png') == 'browse'
    with pytest.raises(ValueError):
        assert opera_rtc_s1_slc._get_file_type('bad_file.zip')


def test_get_message(monkeypatch):
    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456, tzinfo=datetime.UTC)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    assert opera_rtc_s1_slc._get_message({'name': 'test-product'}) == {
        'identifier': 'test-product',
        'collection': 'OPERA_L2_RTC-S1_V1',
        'version': '1.6.1',
        'submissionTime': '2025-02-18T01:02:03.000456Z',
        'product': {'name': 'test-product'},
        'provider': 'HyP3',
        'trace': 'ASF-TOOLS',
    }
    mock_datetime.now.assert_called_once_with(tz=datetime.UTC)


def test_send_messages(sqs_stubber):
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

    opera_rtc_s1_slc._send_messages(
        queue_url='myQueue',
        messages=[
            {'identifier': 'foo'},
            {'identifier': 'bar'},
        ],
    )


def test_process_job(monkeypatch):
    def mock_exists_in_cmr(cmr_domain, short_name, granule_ur, granule_ur_pattern):
        assert cmr_domain == 'test-cmr-domain'
        assert short_name == 'OPERA_L2_RTC-S1_V1'
        assert granule_ur_pattern is opera_rtc_s1_slc._granule_ur_pattern

        assert granule_ur in ('product1', 'product2', 'product3')
        return granule_ur == 'product2'

    monkeypatch.setenv('CMR_DOMAIN', 'test-cmr-domain')
    monkeypatch.setenv('HYP3_CONTENT_BUCKET', 'test-bucket')
    monkeypatch.setenv('QUEUE_URL', 'test-queue-url')

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456, tzinfo=datetime.UTC)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    job = {'job_id': 'test-job'}
    expected_messages = [
        {
            'identifier': 'product1',
            'collection': 'OPERA_L2_RTC-S1_V1',
            'version': '1.6.1',
            'submissionTime': '2025-02-18T01:02:03.000456Z',
            'product': {'name': 'product1'},
            'provider': 'HyP3',
            'trace': 'ASF-TOOLS',
        },
        {
            'identifier': 'product3',
            'collection': 'OPERA_L2_RTC-S1_V1',
            'version': '1.6.1',
            'submissionTime': '2025-02-18T01:02:03.000456Z',
            'product': {'name': 'product3'},
            'provider': 'HyP3',
            'trace': 'ASF-TOOLS',
        },
    ]
    with (
        patch(
            'opera_rtc_s1_slc._get_products',
            return_value=[{'name': 'product1'}, {'name': 'product2'}, {'name': 'product3'}],
        ) as mock_get_products,
        patch('util.exists_in_cmr', mock_exists_in_cmr),
        patch('opera_rtc_s1_slc._send_messages') as mock_send_messages,
    ):
        opera_rtc_s1_slc.process_job(job)

        assert mock_datetime.now.mock_calls == [
            call(tz=datetime.UTC),
            call(tz=datetime.UTC),
        ]
        mock_get_products.assert_called_once_with('test-bucket', 'test-job')
        mock_send_messages.assert_called_once_with('test-queue-url', expected_messages)
