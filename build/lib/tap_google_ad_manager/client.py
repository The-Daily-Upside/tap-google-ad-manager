import requests
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from singer_sdk.streams import RESTStream
from typing import Dict, Optional


class GoogleAdManagerAuthenticator:
    """Custom authenticator for Google Ad Manager using a service account."""

    def __init__(self, key_file_path: str):
        self.credentials = service_account.Credentials.from_service_account_file(
            key_file_path,
            scopes=["https://www.googleapis.com/auth/admanager"]
        )

    @property
    def headers(self) -> Dict[str, str]:
        """Return the authorization headers."""
        if not self.credentials.valid or self.credentials.expired:
            self.credentials.refresh(Request())
        return {"Authorization": f"Bearer {self.credentials.token}"}


class GoogleAdManagerStream(RESTStream):
    """Base stream for Google Ad Manager."""
    url_base = "https://admanager.googleapis.com/v1/"

    def __init__(self, tap, *args, **kwargs):
        super().__init__(tap, *args, **kwargs)
        self.key_file_path = self.config.get("key_file_path")
        if not self.key_file_path:
            raise ValueError("Missing 'key_file_path' in config.")
        self._authenticator = GoogleAdManagerAuthenticator(self.key_file_path)

    def get_url(self, context: dict) -> str:
        """Return the full URL for the stream."""
        network_id = self.config.get("network_id")
        if not network_id:
            raise ValueError("Missing 'network_id' in config.")
        return f"{self.url_base}{self.path.format(network_id=network_id)}"

    @property
    def http_headers(self) -> Dict[str, str]:
        """Return the HTTP headers for the request."""
        return self._authenticator.headers
