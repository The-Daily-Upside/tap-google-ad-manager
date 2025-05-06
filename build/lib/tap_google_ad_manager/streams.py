import time
from typing import Any, Dict, Optional, Iterable
import requests
from json import JSONDecodeError
from tap_google_ad_manager.client import GoogleAdManagerStream
from singer_sdk import typing as th

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
            th.ArrayType(th.ObjectType(
                th.Property("customField", th.StringType),
                th.Property("value", th.StringType),
            ))
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


class ReportResultsStream(GoogleAdManagerStream):
    name = "report_results"
    path = ""
    primary_keys = ["result_name"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("result_name", th.StringType),
        th.Property("report_id", th.StringType),
        th.Property("report_name", th.StringType),
        th.Property("run_time", th.DateTimeType),
        th.Property("rows", th.ObjectType())
    ).to_dict()

    def __init__(self, tap, *args, **kwargs):
        super().__init__(tap, *args, **kwargs)
        self.tap = tap

    def ensure_reports_exist(self, network_id: str, reports: Dict[str, dict]):
        reports_url = f"https://admanager.googleapis.com/v1/networks/{network_id}/reports"

        def fetch_reports():
            resp = self.request_decorator(requests.get)(reports_url, headers=self.http_headers)
            if resp.status_code != 200:
                return {}
            return {r.get("displayName"): r.get("reportId") for r in resp.json().get("reports", [])}

        report_map = fetch_reports()
        for name, spec in reports.items():
            if name not in report_map:
                create_resp = self.request_decorator(requests.post)(
                    reports_url, headers=self.http_headers, json={"displayName": name, **spec})
                if create_resp.status_code != 200:
                    continue
                for attempt in range(MAX_RETRIES):
                    report_map = fetch_reports()
                    if name in report_map:
                        break
                    time.sleep(RETRY_DELAY)
        self.report_display_name_to_id = report_map

    def run_report(self, report_name: str) -> str:
        url = f"https://admanager.googleapis.com/v1/{report_name}:run"
        resp = self.request_decorator(requests.post)(url, headers=self.http_headers)
        return resp.json().get("name")

    def wait_for_completion(self, operation_name: str, poll_interval: float = 5.0, timeout: float = 300.0):
        url = f"https://admanager.googleapis.com/v1/{operation_name}"
        start_time = time.time()
        while time.time() - start_time < timeout:
            resp = self.request_decorator(requests.get)(url, headers=self.http_headers)
            data = resp.json()
            if data.get("done"):
                if "error" in data:
                    raise RuntimeError(f"❌ Report error: {data['error']}")
                return data.get("response", {})
            time.sleep(poll_interval)
        raise TimeoutError("⌛ Timeout: report operation did not complete in time.")

    def fetch_all_rows(self, result_name: str) -> list:
        all_rows = []
        page_token = None
        while True:
            url = f"https://admanager.googleapis.com/v1/{result_name}:fetchRows"
            params = {"pageSize": 1000}
            if page_token:
                params["pageToken"] = page_token
            resp = self.request_decorator(requests.get)(url, headers=self.http_headers, params=params)
            data = resp.json()
            all_rows.extend(data.get("rows", []))
            page_token = data.get("nextPageToken")
            if not page_token:
                break
        return all_rows

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        network_id = self.config.get("network_id")
        report_definitions = self.config.get("reports") or {}
        if not network_id:
            raise ValueError("Missing required config value: 'network_id'")

        self.ensure_reports_exist(network_id, report_definitions)
        display_name_to_id = self.report_display_name_to_id

        for report_key in report_definitions:
            report_id = display_name_to_id.get(report_key)
            if not report_id:
                continue
            report_name = f"networks/{network_id}/reports/{report_id}"
            try:
                operation_name = self.run_report(report_name)
                response = self.wait_for_completion(operation_name)
                result_name = response.get("reportResult")
                if not result_name:
                    continue
                yield {
                    "result_name": result_name,
                    "report_id": report_id,
                    "report_name": report_name,
                    "run_time": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
                    "rows": self.fetch_all_rows(result_name)
                }
            except Exception as e:
                self.logger.error(f"❌ Failed to process report {report_name}: {e}")
