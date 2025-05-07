"""Model registry tools for Hopsworks."""

from fastmcp import Context
from typing import List, Dict, Any, Optional


class ModelRegistryTools:
    """Tools for interacting with Hopsworks Model Registry."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_model_registry)
        self.mcp.tool()(self.list_models)
        self.mcp.tool()(self.get_model)
    
    async def get_model_registry(self, ctx: Context = None) -> Dict[str, Any]:
        """Connect to project's Model Registry.
        
        Returns:
            Model registry information
        """
        if ctx:
            await ctx.info("Getting model registry for current project")
        
        # In real implementation:
        # project = hopsworks.get_current_project()
        # mr = project.get_model_registry()
        
        return {
            "connected": True
        }
    
    async def list_models(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """List models in the Model Registry.
        
        Returns:
            List of models
        """
        if ctx:
            await ctx.info("Listing models in the registry")
            
        # In real implementation:
        # mr = project.get_model_registry()
        # models = mr.get_models()
        
        return []
    
    async def get_model(
        self,
        name: str,
        version: Optional[int] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a specific model from the registry.
        
        Args:
            name: Name of the model
            version: Version of the model (if None, gets the latest)
            
        Returns:
            Model details
        """
        version_info = f"(v{version})" if version else "(latest)"
        if ctx:
            await ctx.info(f"Getting model {name} {version_info}")
            
        # In real implementation:
        # mr = project.get_model_registry()
        # model = mr.get_model(name=name, version=version)
        
        model_version = version or 1
        return {
            "name": name,
            "version": model_version,
            "description": ""
        }