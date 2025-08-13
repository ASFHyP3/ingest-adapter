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


def test_process_message_aria_s1_gunw():
    job = {'job_type': 'ARIA_S1_GUNW'}

    with (
        patch('app.get_job_dict', return_value=job) as mock_get_job_dict,
        patch('aria_s1_gunw.process_job') as mock_process_job,
    ):
        app.process_message(
            {'hyp3_url': 'https://foo.com', 'job_id': 'abc123'},
            {'username': 'myUsername', 'password': 'myPassword'},
        )

        mock_get_job_dict.assert_called_once_with('https://foo.com', 'myUsername', 'myPassword', 'abc123')
        mock_process_job.assert_called_once_with(job)


def test_process_message_unsupported_job_type():
    with patch('app.get_job_dict', return_value={'job_type': 'BAD_JOB_TYPE'}) as mock_get_job_dict:
        with pytest.raises(ValueError, match=r'^Job type BAD_JOB_TYPE is not supported$'):
            app.process_message(
                {'hyp3_url': 'https://bar.com', 'job_id': 'def456'},
                {'username': 'myUsername', 'password': 'myPassword'},
            )
        mock_get_job_dict.assert_called_once_with('https://bar.com', 'myUsername', 'myPassword', 'def456')


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
