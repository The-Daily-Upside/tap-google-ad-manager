"""Google Ad Manager tap class."""

from singer_sdk import Tap
from singer_sdk.typing import PropertiesList, Property, StringType
from tap_google_ad_manager.streams import (
    OrdersStream,
    PlacementsStream,
    ReportsStream,
)

class TapGoogleAdManager(Tap):
    """Singer tap for Google Ad Manager."""
    name = "tap-google-ad-manager"

    config_jsonschema = PropertiesList(
        Property("client_id", StringType, required=True),
        Property("client_secret", StringType, required=True),
        Property("refresh_token", StringType, required=True),
        Property("network_id", StringType, required=True),
    ).to_dict()

    def discover_streams(self):
        """Return a list of discovered streams."""
        return [
            OrdersStream(self),
            PlacementsStream(self),
            ReportsStream(self),
        ]