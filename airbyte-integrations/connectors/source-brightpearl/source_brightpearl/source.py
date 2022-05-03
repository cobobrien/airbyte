#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import urllib.parse
from abc import ABC
from datetime import datetime, timedelta
from itertools import zip_longest
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
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

    batch_size = 200
    cursor_field = None

    def __init__(self, authenticator, config: Dict):
        self.config = config
        self._authenticator = authenticator
        self._session = requests.Session()

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
            return int(response.headers["brightpearl-next-throttle-period"]) / 1000
        else:
            return 5

    def _to_query_param_date(self, datetime: datetime):
        return f"{datetime.year}-{datetime.month}-{datetime.day}"

    def _find_index_of_column(self, items: dict, column_name: str) -> int:
        for index, column in enumerate(items["response"]["metaData"]["columns"]):
            if column["name"] == column_name:
                return index
        raise ValueError("Column name not found")

    def _yield_resource_items(self, response: dict, column_name: str) -> Iterable[Dict]:
        groups = zip_longest(*[iter(response["response"]["results"])] * self.batch_size)
        index_of_resource_id = self._find_index_of_column(items=response, column_name=column_name)
        for it in groups:
            yield [str(s[index_of_resource_id]) for s in it if s is not None]

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params: dict = {}

        if self.cursor_field:
            start_date = stream_slice[self.cursor_field]
            end_date = start_date + timedelta(days=1)
            params.update({self.cursor_field: f"{self._to_query_param_date(start_date)}/{self._to_query_param_date(end_date)}"})

        if next_page_token:
            params.update(**next_page_token)
        payload_str = urllib.parse.urlencode(params, safe="/")

        return payload_str


class IncrementalBrightpearlStream(BrightpearlStream, IncrementalMixin):

    state_checkpoint_interval = 1000

    def __init__(self, authenticator, start_date: datetime, config, **kwargs):
        super().__init__(authenticator=authenticator, config=config)
        self.start_date = start_date
        self._cursor_value = start_date

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return stream_slice["updatedOn"]

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

    def stream_slices(
        self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = (
            datetime.fromisoformat(stream_state[self.cursor_field])
            if stream_state and self.cursor_field in stream_state
            else self.start_date
        )
        return self._chunk_date_range(start_date)


class Orders(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "order-service/order-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for order_ids in self._yield_resource_items(response.json(), column_name="orderId"):

            req = self._create_prepared_request(
                f"{self.url_base}order-service/order/{','.join(order_ids)}", headers=self.authenticator.get_auth_header()
            )
            order_detail_response = self._send_request(req, {})
            yield order_detail_response.json()["response"]


class GoodsInNotes(IncrementalBrightpearlStream):
    primary_key = "batchId"
    cursor_field = "receivedDate"
    batch_ids = set()

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "warehouse-service/goods-in-search"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        """
        Override required because the receivedDate query param corresponds to the receivedOn in the response
        """
        for record in BrightpearlStream.read_records(self, *args, **kwargs):
            if record:
                latest_record_date = datetime.fromisoformat(record[-1]["receivedOn"])
                self._cursor_value = max(self._cursor_value, latest_record_date.replace(tzinfo=None))
            yield from record

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params: str = super().request_params(stream_state, stream_slice, next_page_token)
        return f"{params}&sort={self.primary_key}.ASC" if params else f"sort={self.primary_key}.ASC"

    def prepare_batch_id(self, ids: [int]):
        """
        Required to remove duplicate batch_ids from being queried. Ascending list ordered must be maintained
        """
        prepped_list = list(dict.fromkeys(ids))
        return [x for x in prepped_list if x not in self.batch_ids]

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for batch_ids in self._yield_resource_items(response.json(), column_name="batchId"):
            ids = self.prepare_batch_id(batch_ids)
            if not ids:
                yield []
            req = self._create_prepared_request(
                f"{self.url_base}warehouse-service/order/*/goods-note/goods-in/{','.join(ids)}",
                headers=self.authenticator.get_auth_header(),
            )

            self.batch_ids.update(batch_ids)
            goods_in_notes_detail_response = self._send_request(req, {})
            response = goods_in_notes_detail_response.json()["response"]
            results = []
            for key in response.keys():
                results.append({**{"id": int(key)}, **response[key]})
            yield results


class Products(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/product-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Request: {response.request.url}")
        for product_ids in self._yield_resource_items(response.json(), column_name="productId"):

            req = self._create_prepared_request(
                f"{self.url_base}product-service/product/{','.join(product_ids)}", headers=self.authenticator.get_auth_header()
            )
            product_detail_response = self._send_request(req, {})
            yield product_detail_response.json()["response"]


class GoodsOutNotes(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "createdOn"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "warehouse-service/goods-note/goods-out-search"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for product_type_ids in self._yield_resource_items(response.json(), column_name="goodsOutNoteId"):

            req = self._create_prepared_request(
                f"{self.url_base}warehouse-service/order/*/goods-note/goods-out/{','.join(product_type_ids)}",
                headers=self.authenticator.get_auth_header(),
            )
            goods_out_detail_response = self._send_request(req, {})
            results = []
            response = goods_out_detail_response.json()["response"]
            for key in response.keys():
                results.append({**{"id": int(key)}, **response[key]})
            yield results


class StockCorrections(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "warehouse-service/goods-movement-search"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params: str = super().request_params(stream_state, stream_slice, next_page_token)
        return f"{params}&goodsNoteTypeCode=SC"

    def _yield_resource_items(self, response: dict, column_name1: str, column_name2: str, column_name3: str) -> Iterable[Dict]:
        index_of_resource_id1 = self._find_index_of_column(items=response, column_name=column_name1)
        index_of_resource_id2 = self._find_index_of_column(items=response, column_name=column_name2)
        index_of_resource_id3 = self._find_index_of_column(items=response, column_name=column_name3)
        for result in response["response"]["results"]:
            yield (result[index_of_resource_id1], result[index_of_resource_id2], result[index_of_resource_id3])

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for (warehouseId, goodsNoteId, updatedOn) in self._yield_resource_items(
            response.json(), column_name1="warehouseId", column_name2="goodsNoteId", column_name3="updatedOn"
        ):

            req = self._create_prepared_request(
                f"{self.url_base}warehouse-service/warehouse/{str(warehouseId)}/stock-correction/{str(goodsNoteId)}",
                headers=self.authenticator.get_auth_header(),
            )

            stock_correction_detail_response = self._send_request(req, {})
            response = {"updatedOn": updatedOn}
            response.update(stock_correction_detail_response.json()["response"][0])
            yield [response]


class Brands(BrightpearlStream):
    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/brand-search"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for brand_ids in self._yield_resource_items(response.json(), column_name="brandId"):

            req = self._create_prepared_request(
                f"{self.url_base}product-service/brand/{','.join(brand_ids)}", headers=self.authenticator.get_auth_header()
            )
            brand_detail_response = self._send_request(req, {})
            yield brand_detail_response.json()["response"]


class ProductType(BrightpearlStream):
    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/product-type-search"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for product_type_ids in self._yield_resource_items(response.json(), column_name="id"):

            req = self._create_prepared_request(
                f"{self.url_base}product-service/product-type/{','.join(product_type_ids)}", headers=self.authenticator.get_auth_header()
            )
            product_type_detail_response = self._send_request(req, {})
            yield product_type_detail_response.json()["response"]


class ProductAvailability(BrightpearlStream):
    primary_key = "id"
    http_method = "OPTIONS"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/product"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        pass

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.http_method = "GET"
        for uri in response.json()["response"]["getUris"]:
            req = self._create_prepared_request(
                f"{self.url_base}warehouse-service/{str(uri).replace('/product/', '/product-availability/')}",
                headers=self.authenticator.get_auth_header(),
            )
            product_availability_detail_response = self._send_request(req, {})
            results = []
            response = product_availability_detail_response.json()["response"]
            for key in response.keys():
                results.append({**{"id": key}, **response[key]})
            yield results


class Warehouses(BrightpearlStream):
    primary_key = "id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "warehouse-service/warehouse-search"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params: str = super().request_params(stream_state, stream_slice, next_page_token)
        return f"{params}&{self.primary_key}.ASC" if params else f"sort={self.primary_key}.ASC"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for product_type_ids in self._yield_resource_items(response.json(), column_name="id"):
            req = self._create_prepared_request(
                f"{self.url_base}warehouse-service/warehouse/{','.join(product_type_ids)}", headers=self.authenticator.get_auth_header()
            )
            product_type_detail_response = self._send_request(req, {})
            yield product_type_detail_response.json()["response"]


# Source
class SourceBrightpearl(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            response = requests.get(
                f"https://{config['datacenter']}.brightpearl.com/public-api/{config['account_code']}/integration-service/account-configuration",
                headers={"brightpearl-account-token": config["account_token"], "brightpearl-app-ref": config["app_ref"]},
            )
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        auth = CustomAuth(config=config)
        start_date = datetime.fromisoformat(config["start_date"])

        return [
            Orders(authenticator=auth, start_date=start_date, config=config),
            Products(authenticator=auth, start_date=start_date, config=config),
            Brands(authenticator=auth, config=config),
            ProductType(authenticator=auth, config=config),
            ProductAvailability(authenticator=auth, config=config),
            GoodsOutNotes(authenticator=auth, start_date=start_date, config=config),
            Warehouses(authenticator=auth, config=config),
            StockCorrections(authenticator=auth, start_date=start_date, config=config),
            GoodsInNotes(authenticator=auth, start_date=start_date, config=config),
        ]
