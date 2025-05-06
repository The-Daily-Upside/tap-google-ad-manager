"""Google Ad Manager tap class."""

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList, Property, StringType, ObjectType
from tap_google_ad_manager.streams import (
    OrdersStream,
    PlacementsStream,
    ReportsStream,
    ReportResultsStream,
)

class TapGoogleAdManager(Tap):
    """Singer tap for Google Ad Manager."""
    name = "tap-google-ad-manager"

    config_jsonschema = PropertiesList(
        Property("key_file_path", StringType, required=True),
        Property("network_id", StringType, required=True),
        Property(
            "reports",
            ObjectType(
                additional_properties=True
            ),
            required=True,
        ),
    ).to_dict()
    
    def discover_streams(self):
        """Return a list of discovered streams."""
        key_file_path = self.config.get("key_file_path")
        return [
            OrdersStream(self, key_file_path),
            PlacementsStream(self, key_file_path),
            ReportsStream(self, key_file_path),
            ReportResultsStream(self, key_file_path),
        ]