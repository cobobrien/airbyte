#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from unittest.mock import MagicMock, patch

from source_brightpearl.source import SourceBrightpearl


@patch("requests.get")
def test_check_connection(config):
    source = SourceBrightpearl()
    logger_mock, config_mock = MagicMock(), MagicMock()
    assert source.check_connection(logger_mock, config_mock) == (True, None)


def test_streams(config):
    source = SourceBrightpearl()
    streams = source.streams(config)
    expected_streams_number = 9
    assert len(streams) == expected_streams_number
