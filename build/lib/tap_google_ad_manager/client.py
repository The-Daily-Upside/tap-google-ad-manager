import requests
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import OAuthAuthenticator
from typing import List, Dict

class GoogleAdManagerAuthenticator(OAuthAuthenticator):
    """Custom OAuthAuthenticator for Google Ad Manager."""

    @property
    def oauth_request_body(self) -> Dict[str, str]:
        """Return the OAuth2 request body."""
        return {
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "refresh_token": self.config["refresh_token"],
            "grant_type": "refresh_token",
        }

class GoogleAdManagerStream(RESTStream):
    """Base stream for Google Ad Manager."""
    url_base = "https://admanager.googleapis.com/v1/"

    @property
    def authenticator(self):
        """Return an authenticator for the stream."""
        return GoogleAdManagerAuthenticator(
            stream=self,
            auth_endpoint="https://oauth2.googleapis.com/token",
        )

    def get_url(self, context: dict) -> str:
        """Return the full URL for the stream."""
        network_id = self.config.get("network_id")
        if not network_id:
            raise ValueError("The 'network_id' configuration is missing.")
        return f"{self.url_base}{self.path.format(network_id=network_id)}"