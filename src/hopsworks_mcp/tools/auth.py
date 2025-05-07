"""Authentication tools for Hopsworks."""

from fastmcp import Context
import hopsworks
from typing import Optional, Literal


class AuthTools:
    """Tools for authenticating with Hopsworks."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.login)
        
    async def login(
        self,
        host: str = None, 
        port: int = 443, 
        project: str = None,
        api_key_value: str = None,
        engine: Literal["python", "spark", "hive"] = "python",
        ctx: Context = None
    ) -> dict:
        """Connect to a Hopsworks instance.
        
        Args:
            host: The hostname of the Hopsworks instance
            port: The port on which the Hopsworks instance can be reached
            project: Name of the project to access
            api_key_value: Value of the API Key (should have scopes: featurestore, project, job, kafka)
            engine: The engine to use for data processing (python, spark, or hive)
            
        Returns:
            Connection information
        """
        if ctx:
            await ctx.info(f"Connecting to Hopsworks at {host or 'hopsworks.ai'} using {engine} engine...")
        
        # Perform actual login with the Hopsworks API
        project_instance = hopsworks.login(
            host=host, 
            port=port, 
            project=project, 
            api_key_value=api_key_value,
            engine=engine
        )
        
        return {
            "project_id": project_instance.id,
            "project_name": project_instance.name,
            "host": host,
            "port": port,
            "engine": engine,
            "connected": True
        }