"""Feature store tools for Hopsworks."""

from fastmcp import Context


class FeatureStoreTools:
    """Tools for interacting with Hopsworks Feature Store."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.list_feature_groups)
    
    async def list_feature_groups(self, project_name: str, ctx: Context) -> list:
        """List feature groups in a Hopsworks project.
        
        Args:
            project_name: Name of the Hopsworks project
            
        Returns:
            List of feature groups in the project
        """
        return []