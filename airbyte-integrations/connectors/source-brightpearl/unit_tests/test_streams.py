#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from http import HTTPStatus
from unittest.mock import MagicMock, patch

import pytest
import requests
from source_brightpearl.source import BrightpearlStream


@pytest.fixture
def patch_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(BrightpearlStream, "path", "v0/example_endpoint")
    mocker.patch.object(BrightpearlStream, "primary_key", "test_primary_key")
    mocker.patch.object(BrightpearlStream, "__abstractmethods__", set())


def test_request_params(patch_base_class):
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_params = ""
    assert stream.request_params(**inputs) == expected_params


def test_next_page_token(patch_base_class, requests_mock):
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    requests_mock.get("https://dummy", json={"response": {"metaData": {"morePagesAvailable": True, "lastResult": 10}}})
    inputs = {"response": requests.get("https://dummy")}
    expected_token = {"firstResult": 11}
    assert stream.next_page_token(**inputs) == expected_token


@patch("source_brightpearl.source.BrightpearlStream._send_request")
def test_parse_response(_send_request, patch_base_class, requests_mock):
    requests_mock.get("https://dummy", json={
        "response": {
            "results": [[1, 2, 3], [1, 2, 3]],
            "metaData": {"morePagesAvailable": True, "lastResult": 10, "columns": [{"name": "id"}, {"name": "test"}, {"name": "test2"}]},
        }
    })
    mock_response = MagicMock()
    mock_response.json.return_value = {"response": [{"id": 1, "name": "abc"}]}
    _send_request.return_value = mock_response
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    inputs = {"response": requests.get("https://dummy"), "stream_state": MagicMock()}
    expected_parsed_object = [{"id": 1, "name": "abc"}]
    assert next(stream.parse_response(**inputs)) == expected_parsed_object


def test_request_headers(patch_base_class):
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    inputs = {"stream_slice": None, "stream_state": None, "next_page_token": None}
    expected_headers = {}
    assert stream.request_headers(**inputs) == expected_headers


def test_http_method(patch_base_class):
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    expected_method = "GET"
    assert stream.http_method == expected_method


@pytest.mark.parametrize(
    ("http_status", "should_retry"),
    [
        (HTTPStatus.OK, False),
        (HTTPStatus.BAD_REQUEST, False),
        (HTTPStatus.TOO_MANY_REQUESTS, False),
        (HTTPStatus.INTERNAL_SERVER_ERROR, False),
        (HTTPStatus.SERVICE_UNAVAILABLE, True),
    ],
)
def test_should_retry(patch_base_class, http_status, should_retry):
    response_mock = MagicMock()
    response_mock.status_code = http_status
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    assert stream.should_retry(response_mock) == should_retry


def test_backoff_time(patch_base_class, requests_mock):
    response_mock = MagicMock()
    requests_mock.get("https://dummy", json={}, headers={"brightpearl-next-throttle-period": "56565"})
    inputs = {"response": requests.get("https://dummy")}
    stream = BrightpearlStream(config=MagicMock(), authenticator=MagicMock())
    expected_backoff_time = 56565/1000
    assert stream.backoff_time(**inputs) == expected_backoff_time
