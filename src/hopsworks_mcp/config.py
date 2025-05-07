"""Configuration for the Hopsworks MCP server."""

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings.
    
    Configure with environment variables prefixed with HOPSWORKS_MCP_.
    """
    
    api_url: str = "https://hopsworks.ai"
    
    class Config:
        env_prefix = "HOPSWORKS_MCP_"


settings = Settings()