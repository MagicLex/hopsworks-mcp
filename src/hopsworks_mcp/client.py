"""Client for interacting with Hopsworks API."""

import httpx
from .config import settings


class HopsworksClient:
    """Client for Hopsworks API."""
    
    def __init__(self, api_key: str = None, api_url: str = None):
        self.api_url = api_url or settings.api_url
        self.headers = {}
        
        if api_key:
            self.headers["Authorization"] = f"ApiKey {api_key}"
        
    async def get(self, path: str, **kwargs):
        """Make a GET request to the Hopsworks API."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.api_url}/{path.lstrip('/')}",
                headers=self.headers,
                **kwargs
            )
            response.raise_for_status()
            return response.json()