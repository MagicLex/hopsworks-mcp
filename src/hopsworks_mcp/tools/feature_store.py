"""Feature store tools for Hopsworks."""

from fastmcp import Context
from typing import List, Dict, Any, Optional
import hopsworks


class FeatureStoreTools:
    """Tools for interacting with Hopsworks Feature Store."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_feature_store)
        self.mcp.tool()(self.list_feature_groups)
        self.mcp.tool()(self.get_feature_group)
    
    async def get_feature_store(
        self, 
        project_name: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Connect to project's Feature Store.
        
        Args:
            project_name: Project name of the feature store (defaults to current project)
            
        Returns:
            Feature store information
        """
        if ctx:
            await ctx.info(f"Getting feature store for project: {project_name or 'default'}")
        
        # Get feature store from Hopsworks
        project = hopsworks.get_current_project()
        fs = project.get_feature_store(name=project_name)
        
        return {
            "name": fs.name,
            "connected": True
        }
    
    async def list_feature_groups(
        self, 
        project_name: Optional[str] = None,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """List feature groups in a Hopsworks project.
        
        Args:
            project_name: Name of the Hopsworks project
            
        Returns:
            List of feature groups in the project
        """
        if ctx:
            await ctx.info(f"Listing feature groups for project: {project_name or 'default'}")
            
        # Get feature groups from Hopsworks
        project = hopsworks.get_current_project()
        fs = project.get_feature_store(name=project_name)
        feature_groups = fs.get_feature_groups()
        
        # Convert to serializable format
        result = []
        for fg in feature_groups:
            result.append({
                "name": fg.name,
                "version": fg.version,
                "description": fg.description,
                "created": str(fg.created)
            })
        
        return result
    
    async def get_feature_group(
        self,
        name: str,
        version: int = 1,
        project_name: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a specific feature group.
        
        Args:
            name: Name of the feature group
            version: Version of the feature group
            project_name: Name of the Hopsworks project
            
        Returns:
            Feature group details
        """
        if ctx:
            await ctx.info(f"Getting feature group {name} (v{version}) from project: {project_name or 'default'}")
            
        # Get feature group from Hopsworks
        project = hopsworks.get_current_project()
        fs = project.get_feature_store(name=project_name)
        fg = fs.get_feature_group(name=name, version=version)
        
        # Get features in a serializable format
        features = []
        for feature in fg.features:
            features.append({
                "name": feature.name,
                "type": feature.type,
                "description": feature.description
            })
        
        return {
            "name": fg.name,
            "version": fg.version,
            "description": fg.description,
            "features": features
        }