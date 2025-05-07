"""OpenSearch capability for Hopsworks MCP server."""

from typing import Any, Dict, Optional

import hopsworks
from fastmcp import Context

class OpenSearchTools:
    """Tools for working with OpenSearch in Hopsworks."""

    def __init__(self, mcp):
        """Initialize OpenSearch tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def get_opensearch_py_config(ctx: Context = None) -> Dict[str, Any]:
            """Get the required OpenSearch configuration for opensearch-py library.
            
            Example:
            ```python
            import hopsworks
            from opensearchpy import OpenSearch
            
            project = hopsworks.login()
            opensearch_api = project.get_opensearch_api()
            client = OpenSearch(**opensearch_api.get_default_py_config())
            ```
            
            Returns:
                dict: A dictionary with the required configuration
            """
            if ctx:
                await ctx.info("Getting OpenSearch Python client configuration")
            
            project = hopsworks.get_current_project()
            opensearch_api = project.get_opensearch_api()
            
            config = opensearch_api.get_default_py_config()
            
            return {
                "config": config,
                "status": "success"
            }
        
        @self.mcp.tool()
        async def get_project_index(
            index: str,
            ctx: Context = None
        ) -> Dict[str, str]:
            """Get a project-prefixed OpenSearch index name.
            
            This helper method prefixes the supplied index name with the project name
            to avoid index name clashes.
            
            Args:
                index: The OpenSearch index to interact with
            
            Returns:
                dict: A dictionary containing the prefixed index name
            """
            if ctx:
                await ctx.info(f"Getting project index for: {index}")
            
            project = hopsworks.get_current_project()
            opensearch_api = project.get_opensearch_api()
            
            prefixed_index = opensearch_api.get_project_index(index)
            
            return {
                "index": prefixed_index,
                "original_index": index,
                "status": "success"
            }