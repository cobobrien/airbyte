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
from requests import HTTPError


class BrightpearlAuth:
    def __init__(self, config):
        self.config = config

    def get_auth_header(self):
        return {
            "brightpearl-app-ref": self.config["app_ref"],
            "brightpearl-account-token": self.config["account_token"],
        }


class BrightpearlStream(HttpStream, ABC):

    batch_size = 200
    column_name = "id"
    url_detail = ""
    key_to_flatten = None

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

    def _date_range(self, stream_slice: Mapping[str, any] = None):
        from_date = stream_slice[self.cursor_field]
        to_date = from_date + timedelta(days=1)
        return f"{from_date.isoformat()}/{to_date.isoformat()}"

    def _flatten_keys(self, response: dict):

        """
        The Brightpearl uses IDs as keys rather than values in some cases, making it impossible to create a schema as the keys are dynamic.
        This function modifies the response object to be conform to normal conventions.

        "warehouses": {
            "2": {
                "defaultLocationId": 0,
                "overflowLocationId": 0,
                "reorderLevel": 0,
                "reorderQuantity": 0
            },
            "3": {
                "defaultLocationId": 0,
                "overflowLocationId": 0,
                "reorderLevel": 0,
                "reorderQuantity": 0
            },
            {...}
        }

        Becomes:

        "warehouses": [
            {
                "id": 2
                "defaultLocationId": 0,
                "overflowLocationId": 0,
                "reorderLevel": 0,
                "reorderQuantity": 0
            },
            {
                "id": 3
                "defaultLocationId": 0,
                "overflowLocationId": 0,
                "reorderLevel": 0,
                "reorderQuantity": 0
            },
            {...}
        ]

        """
        response_body = response["response"]
        for index, item in enumerate(response_body):
            results = []
            for key in item[self.key_to_flatten].keys():
                results.append({**{"id": int(key)}, **item[self.key_to_flatten][key]})
            response["response"][index][self.key_to_flatten] = results
        return response

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
    ) -> str:

        params: dict = {}

        if issubclass(type(self), IncrementalMixin):
            params.update({self.cursor_field: self._date_range(stream_slice)})

        if next_page_token:
            params.update(**next_page_token)

        # Required as the `/` between the dates is URL encoded and breaks the request
        payload_str = urllib.parse.urlencode(params, safe="/")

        return payload_str

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for ids in self._yield_resource_items(response.json(), column_name=self.column_name):

            req = self._create_prepared_request(
                f"{self.url_base}{self.url_detail}{','.join(ids)}", headers=self.authenticator.get_auth_header()
            )
            response = self._send_request(req, {}).json()
            if self.key_to_flatten:
                response = self._flatten_keys(response=response)
            yield response["response"]


class IncrementalBrightpearlStream(BrightpearlStream, IncrementalMixin):

    state_checkpoint_interval = 1000
    cursor_field = "updatedOn"

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
        for records in super().read_records(*args, **kwargs):
            for record in records:
                latest_record_date = datetime.fromisoformat(record[self.cursor_field])
                self._cursor_value = max(self._cursor_value, latest_record_date.replace(tzinfo=None))
            yield from records

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
    column_name = "orderId"
    url_detail = "order-service/order/"
    key_to_flatten = "orderRows"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "order-service/order-search"


class GoodsInNotes(IncrementalBrightpearlStream):
    primary_key = "id"
    sort_key = "batchId"
    cursor_field = "receivedDate"
    batch_ids = set()

    """
    Due to inability to retrieve note ids from the goods-in-search response (See: https://api-docs.brightpearl.com/warehouse/goods-in-note/search.html),
    We are using a workaround here, which involves the following:
    1. Fetch results from goods-in-note SEARCH endpoint,
    2. Extract all batchId values into a `set` as it contained multiple duplicates
    (A potential explanation for the duplicates in the initial SEARCH endpoint's response is that one goods-in note can be issued for multiple products in a batch)
    3. use the resulting set of unique ids to fetch from GET.
    The results in response were an object of objects where:
        all the keys in the outer objects were stringified integers that had a corresponding int among the params to the GET endpoint, and
        all the inner obj had their goodsNoteId field equal to the key that inner object was stored at in the outer object
        all the inner objects had their ["goodsMoved"][]["goodsNoteId"] equal to the above and to ["goodsMoved"][]["batchGoodsNoteId"]
    """

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "warehouse-service/goods-in-search"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params: str = super().request_params(stream_state, stream_slice, next_page_token)
        return f"{params}&sort={self.sort_key}.ASC" if params else f"sort={self.sort_key}.ASC"

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

            """
             Two nasty workarounds here, firstly, flattening the response as the GoodsInNotes IDs are used as keys.
             Secondly, the response contains the key `receivedOn` even though the query param is `receivedDate`. This messes up
             the logic around cursor_fields
            """
            for key in response.keys():
                response[key]["receivedDate"] = response[key].pop("receivedOn")
                results.append({**{"id": int(key)}, **response[key]})
            yield results


class Products(IncrementalBrightpearlStream):
    primary_key = "id"
    cursor_field = "updatedOn"
    column_name = "productId"
    url_detail = "product-service/product/"
    key_to_flatten = "warehouses"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/product-search"


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

            """
            Not using the _flatten_keys functionality here as we need some nested flattening and it would be inefficient
            to iterate through the response so twice. Also differs from the `orderRows` in the Orders resource as here,
            they are a nested arrays inside objects as opposed to nested objects inside objects
            """
            results = []
            response = goods_out_detail_response.json()["response"]
            for key in response.keys():
                order_rows = []
                for order_row in response[key]["orderRows"].keys():
                    order_rows.append({**{"id": int(order_row)}, **{"values": response[key]["orderRows"][order_row]}})
                response[key]["orderRows"] = order_rows
                results.append({**{"id": int(key)}, **response[key]})
            yield results


class StockCorrections(IncrementalBrightpearlStream):
    primary_key = "goodsNoteId"
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

    def _yield_resource_items(self, response: dict, column_names: [str]) -> Iterable[Dict]:
        for result in response["response"]["results"]:
            yield (result[self._find_index_of_column(items=response, column_name=column_name)] for column_name in column_names)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        for (warehouseId, goodsNoteId, updatedOn) in self._yield_resource_items(
            response.json(), column_names=["warehouseId", "goodsNoteId", "updatedOn"]
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
    column_name = "brandId"
    url_detail = "product-service/brand/"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/brand-search"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record


class ProductType(BrightpearlStream):
    primary_key = "id"
    column_name = "id"
    url_detail = "product-service/product-type/"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:

        return "product-service/product-type-search"

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            yield from record


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

            try:
                product_availability_detail_response = self._send_request(req, {})
                response = product_availability_detail_response.json()["response"]
            except HTTPError as e:

                def is_non_stock_tracked_products_exception(exception: HTTPError):
                    is_non_stock_exception = filter(
                        lambda x: x == {"code": "WHSC-097", "message": "These are non stock tracked products."}
                        or x == {"code": "WHSC-097", "message": "This is a non stock tracked product."},
                        exception.response.json().get("errors", []),
                    )
                    return any(is_non_stock_exception) and exception.response.status_code == 400

                if e.response.status_code == 404:
                    self.logger.info(f"Product bundle for product id-set {uri} not found or not a bundle")
                    response = {}
                elif is_non_stock_tracked_products_exception(e):
                    self.logger.info(f"Product bundle for product id-set {uri} non stock tracked product")
                    response = {}
                else:
                    raise

            """
            Not using the _flatten_keys functionality here as we need some nested flattening and it would be inefficient
            to iterate through the response so twice.
            """

            results = []
            for key in response.keys():
                warehouses = []
                for warehouse in response[key]["warehouses"].keys():
                    warehouses.append({**{"id": int(warehouse)}, **response[key]["warehouses"][warehouse]})
                response[key]["warehouses"] = warehouses
                results.append({**{"id": int(key)}, **response[key]})
            yield results


class Warehouses(BrightpearlStream):
    primary_key = "id"
    column_name = "id"
    url_detail = "warehouse-service/warehouse/"

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

        auth = BrightpearlAuth(config=config)
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
