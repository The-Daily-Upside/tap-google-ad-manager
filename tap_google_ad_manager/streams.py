import time
import json
from typing import Any, Dict, Optional, Iterable
import requests
from json import JSONDecodeError
from tap_google_ad_manager.client import GoogleAdManagerStream
from singer_sdk import typing as th

# Constants for retry logic
MAX_RETRIES = 5
RETRY_DELAY = 2  # seconds

class OrdersStream(GoogleAdManagerStream):
    name = "orders"
    path = "networks/{network_id}/orders"
    primary_keys = ["orderId"]
    replication_key = "updateTime"
    schema = th.PropertiesList(
        th.Property("orderId", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("startTime", th.DateTimeType),
        th.Property("endTime", th.DateTimeType),
        th.Property("updateTime", th.DateTimeType),
        th.Property("archived", th.BooleanType),
        th.Property("programmatic", th.BooleanType),
        th.Property("trafficker", th.StringType),
        th.Property("advertiser", th.StringType),
        th.Property("agency", th.StringType),
        th.Property("currencyCode", th.StringType),
        th.Property("notes", th.StringType),
        th.Property("poNumber", th.StringType),
        th.Property("status", th.StringType),
        th.Property("salesperson", th.StringType),
        th.Property("secondarySalespeople", th.ArrayType(th.StringType)),
        th.Property("secondaryTraffickers", th.ArrayType(th.StringType)),
        th.Property("appliedLabels", th.ArrayType(th.StringType)),
        th.Property("effectiveTeams", th.ArrayType(th.StringType)),
        th.Property(
            "customFieldValues",
            th.ArrayType(
                th.ObjectType(
                    th.Property("customField", th.StringType),
                    th.Property("value", th.StringType)
                )
            )
        ),
    ).to_dict()

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        yield from response.json().get("orders", [])

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        return {"page_token": next_page_token} if next_page_token else {}

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        return response.json().get("nextPageToken")


class BaseSimpleStream(GoogleAdManagerStream):
    primary_keys = ["name"]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        return response.json().get(self.name, [])

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        return {"page_token": next_page_token} if next_page_token else {}

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        return response.json().get("nextPageToken")


class PlacementsStream(BaseSimpleStream):
    name = "placements"
    path = "networks/{network_id}/placements"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("placementId", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("description", th.StringType),
        th.Property("targetingDescription", th.StringType),
        th.Property("adUnits", th.ArrayType(th.StringType)),
        th.Property("status", th.StringType),
        th.Property("appliedTeams", th.ArrayType(th.StringType)),
        th.Property("updateTime", th.DateTimeType)
    ).to_dict()


class ReportsStream(BaseSimpleStream):
    name = "reports"
    path = "networks/{network_id}/reports"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("reportId", th.StringType),
        th.Property("displayName", th.StringType),
        th.Property("description", th.StringType),
        th.Property("dimensions", th.ArrayType(th.StringType)),
        th.Property("metrics", th.ArrayType(th.StringType)),
        th.Property("filters", th.ArrayType(th.ObjectType())),
        th.Property("updateTime", th.DateTimeType)
    ).to_dict()
