import sys
from unittest.mock import MagicMock, patch

import pytest

import plugin


def test_get_args(monkeypatch):
    args = plugin.get_args(['--hyp3-url', 'foo', '--topic-arn', 'bar', '123'])
    assert args.hyp3_url == 'foo'
    assert args.topic_arn == 'bar'
    assert args.job_id == '123'

    with pytest.raises(ValueError):
        plugin.get_args()

    with pytest.raises(ValueError):
        plugin.get_args(['--hyp3-url', 'foo', '123'])

    with pytest.raises(ValueError):
        plugin.get_args(['--topic-arn', 'bar', '123'])

    with monkeypatch.context() as m:
        m.setenv('HYP3_URL', 'url')
        m.setenv('TOPIC_ARN', 'arn')
        args = plugin.get_args(['321'])
        assert args.hyp3_url == 'url'
        assert args.topic_arn == 'arn'
        assert args.job_id == '321'

    with monkeypatch.context() as m:
        m.setenv('HYP3_URL', 'abc')
        m.setenv('TOPIC_ARN', 'def')
        m.setattr(sys, 'argv', ['foo.py', '234'])
        args = plugin.get_args()
        assert args.hyp3_url == 'abc'
        assert args.topic_arn == 'def'
        assert args.job_id == '234'


def test_publish():
    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        plugin.publish('https://foo.com', 'abc123', 'arn:aws:sns:us-east-1:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-east-1')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-east-1:123456789012:myTopic',
            Message='{"hyp3_api_url": "https://foo.com", "job_id": "abc123"}',
        )

    with patch('boto3.client') as mock_client:
        mock_sns = MagicMock()
        mock_client.return_value = mock_sns

        plugin.publish('https://bar.com', 'def456', 'arn:aws:sns:us-west-2:123456789012:myTopic')

        mock_client.assert_called_once_with('sns', region_name='us-west-2')
        mock_sns.publish.assert_called_once_with(
            TopicArn='arn:aws:sns:us-west-2:123456789012:myTopic',
            Message='{"hyp3_api_url": "https://bar.com", "job_id": "def456"}',
        )
