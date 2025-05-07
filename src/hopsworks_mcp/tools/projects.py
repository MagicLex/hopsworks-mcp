"""Project management tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, List, Optional
import hopsworks


class ProjectTools:
    """Tools for working with Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        # Register tools
        @self.mcp.tool()
        async def get_current_project(ctx: Context = None) -> Dict[str, Any]:
            """Get details about the current project.
            
            Returns:
                dict: Project information including name, id, owner, description, 
                      creation time, and project namespace
            """
            if ctx:
                await ctx.info("Getting current project information")
            
            project = hopsworks.get_current_project()
            
            return {
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
                "project_namespace": project.project_namespace,
                "status": "success"
            }
        
        @self.mcp.tool()
        async def list_projects(ctx: Context = None) -> List[Dict[str, Any]]:
            """List all accessible projects.
            
            Returns:
                list: List of projects with basic information
            """
            if ctx:
                await ctx.info("Listing all accessible projects")
                
            # Note: This is not directly available in the Hopsworks Python client
            # We would need to make a custom API call to get all projects
            # For now, we'll return a list with just the current project
            
            project = hopsworks.get_current_project()
            
            return [{
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
                "status": "success"
            }]
        
        @self.mcp.tool()
        async def create_project(
            name: str,
            description: Optional[str] = None,
            feature_store_topic: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a new project.
            
            Note: This is not supported if you are connected to Serverless Hopsworks.
            
            Example:
            ```python
            import hopsworks
            
            hopsworks.login(...)
            hopsworks.create_project("my_project", description="An example Hopsworks project")
            ```
            
            Args:
                name: The name of the project
                description: Optional description of the project
                feature_store_topic: Optional feature store topic name
                
            Returns:
                dict: Information about the created project
            """
            if ctx:
                await ctx.info(f"Creating project: {name}")
            
            try:
                project = hopsworks.create_project(
                    name=name,
                    description=description,
                    feature_store_topic=feature_store_topic
                )
                
                return {
                    "name": project.name,
                    "id": project.id,
                    "owner": project.owner,
                    "description": project.description,
                    "created": project.created,
                    "project_namespace": project.project_namespace,
                    "status": "created"
                }
            except Exception as e:
                if "Serverless Hopsworks" in str(e):
                    return {
                        "status": "error",
                        "message": "Project creation not supported in Serverless Hopsworks",
                        "error": str(e)
                    }
                return {
                    "status": "error",
                    "message": f"Failed to create project: {name}",
                    "error": str(e)
                }