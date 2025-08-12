import datetime
from unittest.mock import MagicMock, patch

import responses

import aria_s1_gunw


def test_get_granule_ur_pattern():
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    expected = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*'
    assert aria_s1_gunw._get_granule_ur_pattern(granule_ur) == expected

    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    expected = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*'
    assert aria_s1_gunw._get_granule_ur_pattern(granule_ur) == expected


@responses.activate
def test_exists_in_cmr():
    responses.get(
        'https://cmr.earthdata.nasa.gov/search/granules.umm_json',
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {
                    'short_name': 'ARIA_S1_GUNW',
                    'granule_ur': 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*',
                    'options[granule_ur][pattern]': 'true',
                    'page_size': 1,
                },
            ),
        ],
        json={
            'items': [
                {
                    'umm': {
                        'GranuleUR': 'myGranule',
                    },
                },
            ],
        },
    )
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    assert aria_s1_gunw._exists_in_cmr('cmr.earthdata.nasa.gov', granule_ur)

    responses.get(
        'https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json',
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {
                    'short_name': 'ARIA_S1_GUNW',
                    'granule_ur': 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*',
                    'options[granule_ur][pattern]': 'true',
                    'page_size': 1,
                },
            ),
        ],
        json={
            'items': [],
        },
    )
    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    assert not aria_s1_gunw._exists_in_cmr('cmr.uat.earthdata.nasa.gov', granule_ur)


def test_generate_ingest_message(monkeypatch):
    job = {
        'files': [
            {
                's3': {
                    'bucket': 'myBucket',
                    'key': 'myPrefix/myFilename.nc',
                },
            },
        ],
    }
    expected = {
        'ProductName': 'myFilename',
        'DeliveryTime': '2025-02-18T01:02:03.000456',
        'Browse': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.png',
        },
        'Metadata': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.json',
        },
        'Product': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.nc',
        },
    }

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    assert aria_s1_gunw._generate_ingest_message(job) == expected


def test_publish_message():
    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        aria_s1_gunw._publish_message({'ProductName': 'foo'}, 'arn:aws:sns:us-east-1:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-east-1')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:myTopic',
            Message='{"ProductName": "foo"}',
        )

    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        aria_s1_gunw._publish_message({'ProductName': 'bar'}, 'arn:aws:sns:us-west-2:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-west-2')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-west-2:123456789012:myTopic',
            Message='{"ProductName": "bar"}',
        )


def test_process_aria_s1_gunw(monkeypatch):
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('INGEST_TOPIC_ARN', 'myTopicArn')

    now = datetime.datetime(2025, 2, 18, 1, 2, 3, 456)
    mock_datetime = MagicMock(wraps=datetime.datetime)
    mock_datetime.now.return_value = now
    monkeypatch.setattr(datetime, 'datetime', mock_datetime)

    job = {
        'files': [
            {
                's3': {
                    'bucket': 'myBucket',
                    'key': 'myPrefix/myFilename.nc',
                },
            },
        ],
    }
    expected_ingest_message = {
        'ProductName': 'myFilename',
        'DeliveryTime': '2025-02-18T01:02:03.000456',
        'Browse': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.png',
        },
        'Metadata': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.json',
        },
        'Product': {
            'Bucket': 'myBucket',
            'Key': 'myPrefix/myFilename.nc',
        },
    }

    with (
        patch('aria_s1_gunw._exists_in_cmr') as mock_exists_in_cmr,
        patch('aria_s1_gunw._publish_message') as mock_publish_message,
    ):
        mock_exists_in_cmr.return_value = False

        aria_s1_gunw.process_aria_s1_gunw(job)

        mock_exists_in_cmr.assert_called_once_with('cmr.earthdata.nasa.gov', 'myFilename')
        mock_publish_message.assert_called_once_with(expected_ingest_message, 'myTopicArn')

    with (
        patch('aria_s1_gunw._exists_in_cmr') as mock_exists_in_cmr,
        patch('aria_s1_gunw._publish_message') as mock_publish_message,
    ):
        mock_exists_in_cmr.return_value = True

        aria_s1_gunw.process_aria_s1_gunw(job)

        mock_exists_in_cmr.assert_called_once_with('cmr.earthdata.nasa.gov', 'myFilename')
        mock_publish_message.assert_not_called()
