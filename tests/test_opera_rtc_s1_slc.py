import datetime
from unittest.mock import MagicMock

import pytest

import opera_rtc_s1_slc


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


def test_get_products():
    assert False


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


def test_send_messages():
    assert False


def test_process_job():
    assert False
