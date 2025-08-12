import datetime
import json
from unittest.mock import MagicMock, call, patch

import hyp3_sdk
import pytest

import app


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


# TODO: contains logic specific to ARIA_S1_GUNW; mock out process_aria_s1_gunw and test it separately?
def test_process_message(monkeypatch):
    monkeypatch.setenv('CMR_DOMAIN', 'cmr.earthdata.nasa.gov')
    monkeypatch.setenv('INGEST_TOPIC_ARN', 'myTopicArn')
    credentials = {
        'username': 'myUsername',
        'password': 'myPassword',
    }

    with (
        patch('app.get_job_dict', return_value={'job_type': 'ARIA_S1_GUNW'}) as get_job_dict,
        patch(
            'aria_s1_gunw._generate_ingest_message', return_value={'ProductName': 'foo'}
        ) as mock_generate_ingest_message,
        patch('aria_s1_gunw._exists_in_cmr', return_value=False) as mock_exists_in_cmr,
        patch('aria_s1_gunw._publish_message') as mock_publish_message,
    ):
        app.process_message({'hyp3_url': 'https://foo.com', 'job_id': 'abc123'}, credentials)

        get_job_dict.assert_called_once_with('https://foo.com', 'myUsername', 'myPassword', 'abc123')
        mock_generate_ingest_message.assert_called_once_with({'job_type': 'ARIA_S1_GUNW'})
        mock_exists_in_cmr.assert_called_once_with('cmr.earthdata.nasa.gov', 'foo')
        mock_publish_message.assert_called_once_with({'ProductName': 'foo'}, 'myTopicArn')

    with (
        patch('app.get_job_dict', return_value={'job_type': 'ARIA_S1_GUNW'}) as get_job_dict,
        patch(
            'aria_s1_gunw._generate_ingest_message', return_value={'ProductName': 'bar'}
        ) as mock_generate_ingest_message,
        patch('aria_s1_gunw._exists_in_cmr', return_value=True) as mock_exists_in_cmr,
        patch('aria_s1_gunw._publish_message') as mock_publish_message,
    ):
        app.process_message({'hyp3_url': 'https://bar.com', 'job_id': 'def456'}, credentials)

        get_job_dict.assert_called_once_with('https://bar.com', 'myUsername', 'myPassword', 'def456')
        mock_generate_ingest_message.assert_called_once_with({'job_type': 'ARIA_S1_GUNW'})
        mock_exists_in_cmr.assert_called_once_with('cmr.earthdata.nasa.gov', 'bar')
        mock_publish_message.assert_not_called()

    with (
        patch('app.get_job_dict', return_value={'job_type': 'BAD_JOB_TYPE'}) as get_job_dict,
        patch('aria_s1_gunw._generate_ingest_message') as mock_generate_ingest_message,
        patch('aria_s1_gunw._exists_in_cmr') as mock_exists_in_cmr,
        patch('aria_s1_gunw._publish_message') as mock_publish_message,
    ):
        with pytest.raises(ValueError, match=r'^Job type BAD_JOB_TYPE is not supported$'):
            app.process_message({'hyp3_url': 'https://bar.com', 'job_id': 'def456'}, credentials)

        get_job_dict.assert_called_once_with('https://bar.com', 'myUsername', 'myPassword', 'def456')
        mock_generate_ingest_message.assert_not_called()
        mock_exists_in_cmr.assert_not_called()
        mock_publish_message.assert_not_called()


def test_load_credentials(monkeypatch):
    credentials = {'username': 'myUsername', 'password': 'myPassword'}

    with patch('boto3.client') as mock_client, monkeypatch.context() as m:
        m.setenv('SECRET_ARN', 'arn')

        mock_secrets_manager = MagicMock()
        mock_secrets_manager.get_secret_value.return_value = {'SecretString': json.dumps(credentials)}
        mock_client.return_value = mock_secrets_manager

        loaded_creds = app.load_credentials()
        assert loaded_creds == credentials


def test_lambda_handler():
    event = {
        'Records': [
            {'body': '{"Message": "{\\"hyp3_url\\": \\"url1\\", \\"job_id\\": \\"id1\\"}"}'},
            {'body': '{"Message": "{\\"hyp3_url\\": \\"url2\\", \\"job_id\\": \\"id2\\"}"}'},
        ],
    }
    credentials = {'username': 'myUsername', 'password': 'myPassword'}

    with (
        patch('app.process_message') as mock_process_message,
        patch('app.load_credentials', return_value=credentials) as mock_load_credentials,
    ):
        assert app.lambda_handler(event, None) == {'batchItemFailures': []}
        mock_process_message.assert_has_calls(
            [
                call({'hyp3_url': 'url1', 'job_id': 'id1'}, credentials),
                call({'hyp3_url': 'url2', 'job_id': 'id2'}, credentials),
            ],
        )
        mock_load_credentials.assert_called_once_with()

    event = {
        'Records': [
            {'messageId': 'myMessageId', 'body': '{"Message": "bad message"}'},
        ],
    }
    with patch('app.load_credentials', return_value=credentials) as mock_load_credentials:
        assert app.lambda_handler(event, None) == {'batchItemFailures': [{'itemIdentifier': 'myMessageId'}]}
        mock_load_credentials.assert_called_once_with()
