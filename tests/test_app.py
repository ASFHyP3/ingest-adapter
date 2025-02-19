import datetime
from unittest.mock import MagicMock, call, patch

import hyp3_sdk
import responses
import pytest

import app


def test_get_granule_ur_pattern():
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    expected = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*'
    assert app.get_granule_ur_pattern(granule_ur) == expected

    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    expected = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*'
    assert app.get_granule_ur_pattern(granule_ur) == expected


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
    assert app.exists_in_cmr('cmr.earthdata.nasa.gov', granule_ur)

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
    assert not app.exists_in_cmr('cmr.uat.earthdata.nasa.gov', granule_ur)


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

    assert app.generate_ingest_message(job) == expected


def test_publish_message():
    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        app.publish_message({'ProductName': 'foo'}, 'arn:aws:sns:us-east-1:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-east-1')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:myTopic',
            Message='{"ProductName": "foo"}',
        )

    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        app.publish_message({'ProductName': 'bar'}, 'arn:aws:sns:us-west-2:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-west-2')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-west-2:123456789012:myTopic',
            Message='{"ProductName": "bar"}',
        )


def test_get_job_dict():
    job = hyp3_sdk.jobs.Job(
        job_type='myJobType',
        job_id='abc123',
        request_time=datetime.datetime(2025, 2, 19, 1, 2, 3, 456),
        status_code='SUCCEEDED',
        user_id='myUser',
    )

    with patch('hyp3_sdk.HyP3') as mock_constructor:
        mock_hyp3 = MagicMock()
        mock_hyp3.get_job_by_id.return_value = job
        mock_constructor.return_value = mock_hyp3

        job_dict = app.get_job_dict('https://foo.com', 'myUser', 'myPass', 'abc123')

        assert job_dict == {
            'job_type': 'myJobType',
            'job_id': 'abc123',
            'request_time': '2025-02-19T01:02:03',
            'status_code': 'SUCCEEDED',
            'user_id': 'myUser',
        }
        mock_constructor.assert_called_once_with('https://foo.com', 'myUser', 'myPass')
        mock_hyp3.get_job_by_id.assert_called_once_with('abc123')


def test_process_message(monkeypatch):
    monkeypatch.setenv('EDL_USERNAME', 'myUser')
    monkeypatch.setenv('EDL_PASSWORD', 'myPassword')
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('TOPIC_ARN', 'myTopicArn')

    with (
        patch('app.get_job_dict', return_value={'job_type': 'ARIA_S1_GUNW'}) as get_job_dict,
        patch('app.generate_ingest_message', return_value={'ProductName': 'foo'}) as generate_ingest_message,
        patch('app.exists_in_cmr', return_value=False) as exists_in_cmr,
        patch('app.publish_message') as publish_message,
    ):
        app.process_message({'hyp3_url': 'https://foo.com', 'job_id': 'abc123'})

        get_job_dict.assert_called_once_with('https://foo.com', 'myUser', 'myPassword', 'abc123')
        generate_ingest_message.assert_called_once_with({'job_type': 'ARIA_S1_GUNW'})
        exists_in_cmr.assert_called_once_with('foo', 'cmr.earthdata.nasa.gov')
        publish_message.assert_called_once_with({'ProductName': 'foo'}, 'myTopicArn')

    with (
        patch('app.get_job_dict', return_value={'job_type': 'ARIA_S1_GUNW'}) as get_job_dict,
        patch('app.generate_ingest_message', return_value={'ProductName': 'bar'}) as generate_ingest_message,
        patch('app.exists_in_cmr', return_value=True) as exists_in_cmr,
        patch('app.publish_message') as publish_message,
    ):
        app.process_message({'hyp3_url': 'https://bar.com', 'job_id': 'def456'})

        get_job_dict.assert_called_once_with('https://bar.com', 'myUser', 'myPassword', 'def456')
        generate_ingest_message.assert_called_once_with({'job_type': 'ARIA_S1_GUNW'})
        exists_in_cmr.assert_called_once_with('bar', 'cmr.earthdata.nasa.gov')
        publish_message.assert_not_called()

    with (
        patch('app.get_job_dict', return_value={'job_type': 'BAD_JOB_TYPE'}) as get_job_dict,
        patch('app.generate_ingest_message') as generate_ingest_message,
        patch('app.exists_in_cmr') as exists_in_cmr,
        patch('app.publish_message') as publish_message,
    ):
        with pytest.raises(ValueError):
            app.process_message({'hyp3_url': 'https://bar.com', 'job_id': 'def456'})

        get_job_dict.assert_called_once_with('https://bar.com', 'myUser', 'myPassword', 'def456')
        generate_ingest_message.assert_not_called()
        exists_in_cmr.assert_not_called()
        publish_message.assert_not_called()


def test_lambda_handler():
    event = {
        'Records': [
            {'body': '{"Message": "{\\"hyp3_url\\": \\"url1\\", \\"job_id\\": \\"id1\\"}"}'},
            {'body': '{"Message": "{\\"hyp3_url\\": \\"url2\\", \\"job_id\\": \\"id2\\"}"}'},
        ],
    }
    with patch('app.process_message') as mock_process_message:
        assert app.lambda_handler(event, None) == {'batchItemFailures': []}
        mock_process_message.assert_has_calls(
            [
                call({'hyp3_url': 'url1', 'job_id': 'id1'}),
                call({'hyp3_url': 'url2', 'job_id': 'id2'}),
            ],
        )

    event = {
        'Records': [
            {'messageId': 'myMessageId', 'body': '{"Message": "bad message"}'},
        ],
    }
    assert app.lambda_handler(event, None) == {'batchItemFailures': [{'itemIdentifier': 'myMessageId'}]}
