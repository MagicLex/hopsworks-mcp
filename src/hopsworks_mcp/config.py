"""Configuration for the Hopsworks MCP server."""

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings.
    
    Configure with environment variables prefixed with HOPSWORKS_MCP_.
    """
    
    # Hopsworks connection settings
    hopsworks_host: str = ""
    hopsworks_port: int = 443
    hopsworks_project: str = ""
    hopsworks_api_key: str = ""
    hopsworks_hostname_verification: bool = False
    
    class Config:
        env_prefix = "HOPSWORKS_MCP_"


settings = Settings()