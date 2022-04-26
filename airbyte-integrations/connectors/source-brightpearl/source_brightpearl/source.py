import urllib.parse
from abc import ABC
from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict

from itertools import zip_longest

from airbyte_cdk.sources.streams import IncrementalMixin
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream



class CustomAuth:

    def __init__(self, config):
        self.config = config


    def get_auth_header(self):
        return {
            "brightpearl-app-ref": self.config["app_ref"],
            "brightpearl-account-token": self.config["account_token"],
        }


class BrightpearlStream(HttpStream, ABC):


    def __init__(self, authenticator, config: Dict):
        self.config = config
        self._authenticator = authenticator


    @property
    def url_base(self) -> str:
        return f"https://{self.config['datacenter']}.brightpearl.com/public-api/{self.config['account_code']}/"


    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:

        response = response.json()["response"]["metaData"]
        if response["morePagesAvailable"]:
            return {"firstResult": response["lastResult"] + 1}
        else:
            return None

    def should_retry(self, response: requests.Response) -> bool:
        return response.status_code == 503

    def backoff_time(self, response: requests.Response) -> Optional[float]:

        if "brightpearl-next-throttle-period" in response.headers:
            return (int(response.headers["brightpearl-next-throttle-period"])/1000)
        else:
            return 5

    def _to_query_param_date(self, datetime: datetime):
        return f"{datetime.year}-{datetime.month}-{datetime.day}"


    def _find_index_of_column(self, items: dict, column_name: str) -> int:
        for index, column in enumerate(items["response"]["metaData"]["columns"]):
            if column["name"] == column_name:
                return index
        raise ValueError("Column name not found")

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        yield {}



class IncrementalBrightpearlStream(BrightpearlStream, IncrementalMixin):

    state_checkpoint_interval = 1000
    batch_size = 200

    def __init__(self, authenticator, start_date: datetime, config, **kwargs):
        super().__init__(authenticator=authenticator, config=config)
        self.start_date = start_date
        self._cursor_value = start_date
        self._session = requests.Session()

    def path(self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None,
             next_page_token: Mapping[str, Any] = None) -> str:
        return stream_slice['updatedOn']

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            return {self.cursor_field: self.start_date}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = datetime.fromisoformat(value[self.cursor_field])

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            latest_record_date = datetime.fromisoformat(record[-1][self.cursor_field])
            self._cursor_value = max(self._cursor_value, latest_record_date.replace(tzinfo=None))
            yield from record

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        """
        Returns a list of each day between the start date and now.
        The return value is a list of dicts {'date': date_string}.
        """
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_field: start_date})
            start_date += timedelta(days=1)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.fromisoformat(stream_state[self.cursor_field]) if stream_state and self.cursor_field in stream_state else self.start_date
        return self._chunk_date_range(start_date)

    def _yield_resource_items(self, response: dict, column_name: str) -> Iterable[Dict]:
        groups = zip_longest(*[iter(response["response"]["results"])] * self.batch_size)
        index_of_order_id = self._find_index_of_column(items=response, column_name=column_name)
        for it in groups:
            yield [str(s[index_of_order_id]) for s in it if s is not None]


    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        start_date = stream_slice[self.cursor_field]
        end_date = start_date + timedelta(days=1)
        params = {self.cursor_field : f"{self._to_query_param_date(start_date)}/{self._to_query_param_date(end_date)}"}
        if next_page_token:
            params.update(**next_page_token)
        payload_str = urllib.parse.urlencode(params, safe='/')

        return payload_str


class Orders(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"


    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return f"order-service/order-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for order_ids in self._yield_resource_items(response.json(), column_name="orderId"):

            req = self._create_prepared_request(f"{self.url_base}order-service/order/{','.join(order_ids)}",
                                                headers=self.authenticator.get_auth_header())
            order_detail_response = self._send_request(req, {})
            yield order_detail_response.json()["response"]

class Products(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return f"product-service/product-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for product_ids in self._yield_resource_items(response.json(), column_name="productId"):

            req = self._create_prepared_request(f"{self.url_base}product-service/product/{','.join(product_ids)}",
                                                headers=self.authenticator.get_auth_header())
            product_detail_response = self._send_request(req, {})
            yield product_detail_response.json()["response"]


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
        start_date = datetime.fromisoformat(config['start_date'])

        orders = Orders(authenticator=auth, start_date=start_date, config=config)
        products = Products(authenticator=auth, start_date=start_date, config=config)

        # TODO remove the authenticator if not required.
        return [orders, products]
