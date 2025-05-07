"""Model serving tools for Hopsworks."""

from fastmcp import Context
from typing import List, Dict, Any, Optional
import hopsworks


class ModelServingTools:
    """Tools for interacting with Hopsworks Model Serving."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_model_serving)
        self.mcp.tool()(self.list_deployments)
        self.mcp.tool()(self.deploy_model)
    
    async def get_model_serving(self, ctx: Context = None) -> Dict[str, Any]:
        """Connect to project's Model Serving.
        
        Returns:
            Model serving information
        """
        if ctx:
            await ctx.info("Getting model serving for current project")
        
        project = hopsworks.get_current_project()
        ms = project.get_model_serving()
        
        return {
            "connected": True
        }
    
    async def list_deployments(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """List deployed models.
        
        Returns:
            List of deployments
        """
        if ctx:
            await ctx.info("Listing model deployments")
            
        project = hopsworks.get_current_project()
        ms = project.get_model_serving()
        deployments = ms.get_deployments()
        
        result = []
        for deployment in deployments:
            result.append({
                "name": deployment.name,
                "model_name": deployment.model_name,
                "model_version": deployment.model_version,
                "status": deployment.status,
                "created": str(deployment.created)
            })
        
        return result
    
    async def deploy_model(
        self,
        model_name: str,
        model_version: int,
        deployment_name: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Deploy a model to the serving infrastructure.
        
        Args:
            model_name: Name of the model
            model_version: Version of the model
            deployment_name: Name for the deployment (defaults to model name)
            
        Returns:
            Deployment information
        """
        serving_name = deployment_name or model_name
        
        if ctx:
            await ctx.info(f"Deploying model {model_name} (v{model_version}) as '{serving_name}'")
            
        project = hopsworks.get_current_project()
        ms = project.get_model_serving()
        mr = project.get_model_registry()
        model = mr.get_model(name=model_name, version=model_version)
        deployment = ms.create_deployment(name=serving_name, model=model)
        
        return {
            "name": deployment.name,
            "model": {
                "name": model.name,
                "version": model.version
            },
            "status": deployment.status,
            "endpoint": deployment.endpoint
        }