#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import pytest


@pytest.fixture
def config():
    return {
        "datacenter": "ws-test",
        "app_ref": "test-1",
        "account_token": "testtes+settesttest=",
        "account_code": "test",
        "start_date": "2022-05-03",
    }
