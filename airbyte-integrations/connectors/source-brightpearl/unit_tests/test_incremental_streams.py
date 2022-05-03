#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from datetime import datetime
from unittest.mock import MagicMock

from airbyte_cdk.models import SyncMode
from pytest import fixture
from source_brightpearl.source import IncrementalBrightpearlStream


@fixture
def patch_incremental_base_class(mocker):
    # Mock abstract methods to enable instantiating abstract class
    mocker.patch.object(IncrementalBrightpearlStream, "path", "v0/example_endpoint")
    mocker.patch.object(IncrementalBrightpearlStream, "primary_key", "test_primary_key")
    mocker.patch.object(IncrementalBrightpearlStream, "__abstractmethods__", set())


def test_cursor_field(patch_incremental_base_class):
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-03"))
    expected_cursor_field = "updatedOn"
    assert stream.cursor_field == expected_cursor_field


def test_get_updated_state(patch_incremental_base_class):
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-03"))
    inputs = {"current_stream_state": None, "latest_record": None}
    expected_state = {}
    assert stream.get_updated_state(**inputs) == expected_state


def test_stream_slices(patch_incremental_base_class):
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-01"))
    inputs = {"sync_mode": SyncMode.incremental, "cursor_field": [], "stream_state": {}}
    expected_stream_slice = [
        {"updatedOn": datetime.fromisoformat("2022-05-01")},
        {"updatedOn": datetime.fromisoformat("2022-05-02")},
        {"updatedOn": datetime.fromisoformat("2022-05-03")},
    ]
    assert stream.stream_slices(**inputs) == expected_stream_slice


def test_supports_incremental(patch_incremental_base_class, mocker):
    mocker.patch.object(IncrementalBrightpearlStream, "cursor_field", "dummy_field")
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-01"))
    assert stream.supports_incremental


def test_source_defined_cursor(patch_incremental_base_class):
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-01"))
    assert stream.source_defined_cursor


def test_stream_checkpoint_interval(patch_incremental_base_class):
    stream = IncrementalBrightpearlStream(config=MagicMock(), authenticator=MagicMock(), start_date=datetime.fromisoformat("2022-05-01"))
    expected_checkpoint_interval = 1000
    assert stream.state_checkpoint_interval == expected_checkpoint_interval
