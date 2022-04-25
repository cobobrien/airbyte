#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import tempfile
import urllib.parse
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict
from airbyte_cdk.sources.streams.http.auth import NoAuth
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.models import SyncMode

from airbyte_cdk.sources.streams import IncrementalMixin
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""


class CustomAuth:

    def __init__(self, config):
        self.config = config


    def get_auth_header(self):
        return {
            "brightpearl-app-ref": self.config["app_ref"],
            "brightpearl-account-token": self.config["account_token"],
        }


# Basic full refresh stream
class BrightpearlStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class BrightpearlStream(HttpStream, ABC)` which is the current class
    `class Customers(BrightpearlStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(BrightpearlStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalBrightpearlStream((BrightpearlStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    def __init__(self, authenticator, config: Dict):
        self.config = config
        self._authenticator = authenticator


    @property
    def url_base(self) -> str:
        return f"https://{self.config['datacenter']}.brightpearl.com/public-api/{self.config['account_code']}/"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        response = response.json()["response"]["metaData"]
        self.logger.info(f"response: {response}")
        if response["morePagesAvailable"]:
            return {"firstResult": response["lastResult"] + 1}
        else:
            return None

    def should_retry(self, response: requests.Response) -> bool:
        self.logger.info(f"Headers: {response.headers} Status: {response.status_code}")

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        """This method is called if we run into the rate limit.
        Slack puts the retry time in the `Retry-After` response header so we
        we return that value. If the response is anything other than a 429 (e.g: 5XX)
        fall back on default retry behavior.
        Rate Limits Docs: https://api.slack.com/docs/rate-limits#web"""

        self.logger.info("Rate Limit!!!")

        if "brightpearl-next-throttle-period" in response.headers:
            return (response.headers["brightpearl-next-throttle-period"]/1000)
        else:
            self.logger.info("Retry-after header not found. Using default backoff value")
            return 5



    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}



# Basic incremental stream
class IncrementalBrightpearlStream(BrightpearlStream, IncrementalMixin):

    state_checkpoint_interval = 1000
    cursor_field = "updatedOn"

    def __init__(self, authenticator, start_date: datetime, config, **kwargs):
        super().__init__(authenticator=authenticator, config=config)
        self.start_date = start_date
        self._cursor_value = None
        self._session = requests.Session()

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
             next_page_token: Mapping[str, Any] = None) -> str:
        return stream_slice['updatedOn']

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value.strftime('%Y-%m-%d')}
        else:
            return {self.cursor_field: self.start_date.strftime('%Y-%m-%d')}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.strptime(value[self.cursor_field], '%Y-%m-%d')

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = datetime.strptime(record[self.cursor_field], '%Y-%m-%d')
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date.strftime('%Y-%m-%d')})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], '%Y-%m-%d') if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)





class Orders(IncrementalBrightpearlStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "id"


    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        blah_date = datetime.strptime(stream_slice['updatedOn'],'%Y-%m-%d')
        to_date = datetime.strptime(str(blah_date + timedelta(days=1)),'%Y-%m-%d %H:%M:%S')
        self.logger.info(f"to_date: {to_date}")
        self.logger.info(f"stream_slice: {stream_slice}")
        params = {"updatedOn": f"{stream_slice['updatedOn']}/{to_date.year}-{to_date.month}-{to_date.day}"}
        if next_page_token:
            params.update(**next_page_token)
        payload_str = urllib.parse.urlencode(params, safe='/')

        return payload_str


    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return f"order-service/order-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"request URL: {response.request.url}")
        # for order in response.json()["response"]["results"]:
        #     order_detail_response = requests.get(f"{self.url_base}order-service/order/{order[0]}", headers=self.authenticator.get_auth_header())
            # self.logger.info(f"RESULT: {order_detail_response.json() }")
        yield response.json()["response"]

class Order(HttpSubStream, BrightpearlStream):

    primary_key = "id"

    http_method = "GET"


    def path(self, **kwargs) -> str:
        self.logger.info("HEEREREREHERERER")

        return f"order-service/order/"


    # def stream_slices(
    #     self,
    #     sync_mode: SyncMode,
    #     cursor_field: List[str] = None,
    #     stream_state: Mapping[str, Any] = None,
    # ) -> Iterable[Optional[Mapping[str, Any]]]:
    #     self.logger.info("HEEREREREHERERER")
    #
    #     # gather parent stream records in full
    #     slices = list(super().stream_slices(SyncMode.full_refresh, cursor_field, stream_state))
    #     self.logger.info("HEEREREREHERERER")
    #     self.is_finished = False
    #     for page in slices:
    #         for result in page:
    #             order_id = result[0]
    #             self.order_ids.append(order_id)
    #
    #             self.is_finished = page["metaData"]["morePagesAvailable"] == False
    #
    #             yield {"order_id": order_id}


    # def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
    #     self.logger.info("HEEREREREHERERER")



# Source
class SourceBrightpearl(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            response = requests.get(f"https://{config['datacenter']}.brightpearl.com/public-api/{config['account_code']}/integration-service/account-configuration", headers={"brightpearl-account-token": config['account_token'], "brightpearl-app-ref": config['app_ref']})
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        auth = CustomAuth(config=config)
        start_date = datetime.strptime(config['start_date'], '%Y-%m-%d')

        orders = Orders(authenticator=auth, start_date=start_date, config=config)
        order = Order(parent=orders, authenticator=auth, config=config)
        # TODO remove the authenticator if not required.
        return [orders, order]
