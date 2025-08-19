from unittest.mock import MagicMock, patch

import gunw


def test_granule_ur_pattern():
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    expected = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*'
    assert gunw._granule_ur_pattern(granule_ur) == expected

    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    expected = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*'
    assert gunw._granule_ur_pattern(granule_ur) == expected


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

    assert gunw._generate_ingest_message(job) == expected


def test_publish_message():
    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        gunw._publish_message({'ProductName': 'foo'}, 'arn:aws:sns:us-east-1:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-east-1')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:myTopic',
            Message='{"ProductName": "foo"}',
        )

    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        gunw._publish_message({'ProductName': 'bar'}, 'arn:aws:sns:us-west-2:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-west-2')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-west-2:123456789012:myTopic',
            Message='{"ProductName": "bar"}',
        )


def test_process_job(monkeypatch):
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('INGEST_TOPIC_ARN', 'myTopicArn')

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
        patch('util.exists_in_cmr', return_value=False) as mock_exists_in_cmr,
        patch('gunw._publish_message') as mock_publish_message,
    ):
        gunw.process_job(job)

        mock_exists_in_cmr.assert_called_once_with(
            'cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', 'myFilename', gunw._granule_ur_pattern
        )
        mock_publish_message.assert_called_once_with(expected_ingest_message, 'myTopicArn')

    with (
        patch('util.exists_in_cmr', return_value=True) as mock_exists_in_cmr,
        patch('gunw._publish_message') as mock_publish_message,
    ):
        gunw.process_job(job)

        mock_exists_in_cmr.assert_called_once_with(
            'cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', 'myFilename', gunw._granule_ur_pattern
        )
        mock_publish_message.assert_not_called()
