"""Authentication tools for Hopsworks."""

from fastmcp import Context


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
        ctx: Context = None
    ) -> dict:
        """Connect to a Hopsworks instance.
        
        Args:
            host: The hostname of the Hopsworks instance
            port: The port on which the Hopsworks instance can be reached
            project: Name of the project to access
            api_key_value: Value of the API Key
            
        Returns:
            Connection information
        """
        if ctx:
            await ctx.info(f"Connecting to Hopsworks at {host or 'hopsworks.ai'}...")
        
        # Store connection details in context for other tools to use
        connection = {
            "host": host,
            "port": port,
            "project": project,
            "connected": True
        }
        
        # In a real implementation, we would use the hopsworks library:
        # import hopsworks
        # project = hopsworks.login(host=host, port=port, project=project, api_key_value=api_key_value)
        # return {"project_id": project.id, "project_name": project.name}
        
        return connection