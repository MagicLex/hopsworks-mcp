"""Feature store capability for Hopsworks MCP server."""

from fastmcp import Context
from typing import List, Dict, Any, Optional, Union
import json
import hopsworks


class FeatureStoreTools:
    """Tools for interacting with Hopsworks Feature Store."""

    def __init__(self, mcp):
        """Initialize feature store tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        # Register tools
        @self.mcp.tool()
        async def get_feature_store(
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Connect to project's Feature Store.
            
            Args:
                project_name: Project name of the feature store (defaults to current project)
                
            Returns:
                dict: Feature store information
            """
            if ctx:
                await ctx.info(f"Getting feature store for project: {project_name or 'default'}")
            
            try:
                # Get feature store from Hopsworks
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                return {
                    "name": fs.name,
                    "id": fs.id,
                    "project_name": fs.project_name,
                    "project_id": fs.project_id,
                    "online_enabled": fs.online_enabled,
                    "offline_featurestore_name": fs.offline_featurestore_name,
                    "online_featurestore_name": fs.online_featurestore_name,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature store: {str(e)}"
                }
        
        @self.mcp.tool()
        async def execute_feature_store_query(
            query: str,
            dataframe_type: str = "pandas",
            online: bool = False,
            read_options: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            limit: int = 100,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Execute SQL query on the feature store.
            
            Args:
                query: SQL query to execute
                dataframe_type: Type of dataframe to return (pandas, spark)
                online: Whether to query the online feature store
                read_options: Additional options for the query
                project_name: Name of the Hopsworks project's feature store
                limit: Maximum number of rows to return
                
            Returns:
                dict: Query results in JSON format
            """
            if ctx:
                await ctx.info(f"Executing feature store query: {query}")
                
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                if read_options is None:
                    read_options = {}
                
                # Execute query
                result_df = fs.sql(
                    query=query, 
                    dataframe_type=dataframe_type, 
                    online=online, 
                    read_options=read_options
                )
                
                # Convert result to JSON (handle both pandas and spark dataframes)
                if dataframe_type == "spark":
                    # Spark dataframe
                    result_rows = result_df.limit(limit).toJSON().collect()
                    rows = [json.loads(row) for row in result_rows]
                else:
                    # Pandas dataframe
                    rows = json.loads(result_df.head(limit).to_json(orient="records"))
                
                return {
                    "rows": rows,
                    "count": len(rows),
                    "truncated": len(rows) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to execute query: {str(e)}"
                }
                

# Helper function to get feature view names
async def _get_feature_view_names(project, fs):
    """Get names of feature views in the feature store.
    
    This uses SQL query to get feature view names since there's no direct API.
    
    Args:
        project: Hopsworks project
        fs: Feature store instance
        
    Returns:
        list: List of feature view names
    """
    try:
        # Try to get feature view names using SQL query on the metadata
        query = "SELECT name FROM feature_store_metadata.feature_views GROUP BY name"
        df = fs.sql(query, dataframe_type="pandas")
        return df['name'].tolist()
    except:
        # Fallback to empty list if query fails
        return []