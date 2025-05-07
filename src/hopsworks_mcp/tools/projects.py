"""Project management tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, List
import hopsworks


class ProjectTools:
    """Tools for working with Hopsworks projects."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_current_project)
        self.mcp.tool()(self.list_projects)
        
    async def get_current_project(self, ctx: Context = None) -> Dict[str, Any]:
        """Get details about the current project.
        
        Returns:
            Project information
        """
        if ctx:
            await ctx.info("Getting current project information")
        
        project = hopsworks.get_current_project()
        
        return {
            "name": project.name,
            "id": project.id,
            "owner": project.owner
        }
    
    async def list_projects(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """List all accessible projects.
        
        Returns:
            List of projects
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
            "owner": project.owner
        }]