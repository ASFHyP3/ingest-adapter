import sys

import pytest

import publish_container


def test_get_args(monkeypatch):
    args = publish_container.get_args(['--hyp3-url', 'foo', '--topic-arn', 'bar', '123'])
    assert args.hyp3_url == 'foo'
    assert args.topic_arn == 'bar'
    assert args.job_id == '123'

    with pytest.raises(ValueError):
        publish_container.get_args()

    with pytest.raises(ValueError):
        publish_container.get_args(['--hyp3-url', 'foo', '123'])

    with pytest.raises(ValueError):
        publish_container.get_args(['--topic-arn', 'bar', '123'])

    with monkeypatch.context() as m:
        m.setenv('HYP3_URL', 'url')
        m.setenv('TOPIC_ARN', 'arn')
        args = publish_container.get_args(['321'])
        assert args.hyp3_url == 'url'
        assert args.topic_arn == 'arn'
        assert args.job_id == '321'

    with monkeypatch.context() as m:
        m.setenv('HYP3_URL', 'abc')
        m.setenv('TOPIC_ARN', 'def')
        m.setattr(sys, 'argv', ['foo.py', '234'])
        args = publish_container.get_args()
        assert args.hyp3_url == 'abc'
        assert args.topic_arn == 'def'
        assert args.job_id == '234'


def test_publish():
    assert False
