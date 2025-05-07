"""Environment management tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks


class EnvironmentTools:
    """Tools for working with Hopsworks Python environments."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_environment_api)
        self.mcp.tool()(self.create_environment)
        self.mcp.tool()(self.get_environment)
        self.mcp.tool()(self.delete_environment)
        self.mcp.tool()(self.install_requirements)
        self.mcp.tool()(self.install_wheel)
        
    async def get_environment_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the Python environment API for the project.
        
        Returns:
            Environment API information
        """
        if ctx:
            await ctx.info("Getting Python environment API for current project")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        
        return {"connected": True}
    
    async def create_environment(
        self,
        name: str,
        description: Optional[str] = None,
        base_environment_name: str = "python-feature-pipeline",
        await_creation: bool = True,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create Python environment for the project.
        
        Args:
            name: Name of the environment
            description: Description of the environment
            base_environment_name: The name of the environment to clone from
            await_creation: If True, wait until creation is finished
            
        Returns:
            Environment information
        """
        if ctx:
            await ctx.info(f"Creating Python environment: {name}")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        env = env_api.create_environment(
            name=name,
            description=description,
            base_environment_name=base_environment_name,
            await_creation=await_creation
        )
        
        return {
            "name": env.name,
            "id": env.id,
            "status": "created"
        }
    
    async def get_environment(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get handle for a Python environment in the project.
        
        Args:
            name: Name of the environment
            
        Returns:
            Environment information
        """
        if ctx:
            await ctx.info(f"Getting Python environment: {name}")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        env = env_api.get_environment(name)
        
        if not env:
            return {
                "name": name,
                "exists": False
            }
            
        return {
            "name": env.name,
            "id": env.id,
            "exists": True
        }
    
    async def delete_environment(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Delete a Python environment.
        
        Args:
            name: Name of the environment to delete
            
        Returns:
            Deletion status
        """
        if ctx:
            await ctx.info(f"Deleting Python environment: {name}")
            await ctx.info("WARNING: This is a potentially dangerous operation")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        env = env_api.get_environment(name)
        
        if not env:
            return {
                "name": name,
                "status": "not_found"
            }
            
        env.delete()
        
        return {
            "name": name,
            "status": "deleted"
        }
    
    async def install_requirements(
        self,
        environment_name: str,
        path: str,
        await_installation: bool = True,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Install libraries specified in a requirements.txt file.
        
        Args:
            environment_name: Name of the environment
            path: The path on Hopsworks where the requirements.txt file is located
            await_installation: If True, wait until installation is finished
            
        Returns:
            Installation status
        """
        if ctx:
            await ctx.info(f"Installing requirements from {path} in environment: {environment_name}")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        env = env_api.get_environment(environment_name)
        
        if not env:
            return {
                "environment": environment_name,
                "status": "environment_not_found"
            }
            
        library = env.install_requirements(path, await_installation=await_installation)
        
        return {
            "environment": environment_name,
            "library": library.name if library else None,
            "status": "installed" if await_installation else "installing"
        }
    
    async def install_wheel(
        self,
        environment_name: str,
        path: str,
        await_installation: bool = True,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Install a Python library packaged in a wheel file.
        
        Args:
            environment_name: Name of the environment
            path: The path on Hopsworks where the wheel file is located
            await_installation: If True, wait until installation is finished
            
        Returns:
            Installation status
        """
        if ctx:
            await ctx.info(f"Installing wheel from {path} in environment: {environment_name}")
        
        project = hopsworks.get_current_project()
        env_api = project.get_environment_api()
        env = env_api.get_environment(environment_name)
        
        if not env:
            return {
                "environment": environment_name,
                "status": "environment_not_found"
            }
            
        library = env.install_wheel(path, await_installation=await_installation)
        
        return {
            "environment": environment_name,
            "library": library.name if library else None,
            "status": "installed" if await_installation else "installing"
        }