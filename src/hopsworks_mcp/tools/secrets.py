"""Secrets management capability for Hopsworks MCP server."""

from typing import Any, Dict, List, Optional

import hopsworks
from fastmcp import Context


class SecretsTools:
    """Tools for working with Hopsworks secrets."""

    def __init__(self, mcp):
        """Initialize secrets management tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_secret(
            name: str,
            value: str,
            project: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a new secret.
            
            Example:
            ```python
            import hopsworks
            project = hopsworks.login()
            secrets_api = hopsworks.get_secrets_api()
            secret = secrets_api.create_secret("my_secret", "Fk3MoPlQXCQvPo")
            ```
            
            Args:
                name: Name of the secret
                value: The secret value
                project: Name of the project to share the secret with
                
            Returns:
                dict: Information about the created secret
            """
            if ctx:
                await ctx.info(f"Creating secret: {name}")
            
            try:
                secrets_api = hopsworks.get_secrets_api()
                secret = secrets_api.create_secret(name=name, value=value, project=project)
                
                return {
                    "name": secret.name,
                    "owner": secret.owner,
                    "created": secret.created,
                    "scope": secret.scope,
                    "visibility": secret.visibility,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create secret: {name}",
                    "error": str(e)
                }
        
        @self.mcp.tool()
        async def get_secret_value(
            name: str,
            owner: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a secret's value.
            
            If the secret does not exist and the application is running interactively,
            it prompts the user to create the secret.
            
            Args:
                name: Name of the secret
                owner: Email of the owner for a secret shared with the current project
                
            Returns:
                dict: The secret value or error information
            """
            if ctx:
                await ctx.info(f"Getting secret value: {name}")
            
            try:
                secrets_api = hopsworks.get_secrets_api()
                value = secrets_api.get(name=name, owner=owner)
                
                return {
                    "name": name,
                    "value": value,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get secret value: {name}",
                    "error": str(e)
                }
        
        @self.mcp.tool()
        async def get_secret(
            name: str,
            owner: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a secret's metadata.
            
            Args:
                name: Name of the secret
                owner: Username of the owner for a secret shared with the current project
                
            Returns:
                dict: Secret information or error information
            """
            if ctx:
                await ctx.info(f"Getting secret metadata: {name}")
            
            try:
                secrets_api = hopsworks.get_secrets_api()
                secret = secrets_api.get_secret(name=name, owner=owner)
                
                if secret:
                    return {
                        "name": secret.name,
                        "owner": secret.owner,
                        "created": secret.created,
                        "scope": secret.scope,
                        "visibility": secret.visibility,
                        "status": "success"
                    }
                return {
                    "status": "not_found",
                    "message": f"Secret not found: {name}"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get secret: {name}",
                    "error": str(e)
                }
        
        @self.mcp.tool()
        async def list_secrets(ctx: Context = None) -> List[Dict[str, Any]]:
            """List all accessible secrets.
            
            Returns:
                list: List of secret information
            """
            if ctx:
                await ctx.info("Listing all accessible secrets")
            
            try:
                secrets_api = hopsworks.get_secrets_api()
                secrets = secrets_api.get_secrets()
                
                return [
                    {
                        "name": secret.name,
                        "owner": secret.owner,
                        "created": secret.created,
                        "scope": secret.scope,
                        "visibility": secret.visibility
                    }
                    for secret in secrets
                ]
            except Exception as e:
                return [{
                    "status": "error",
                    "message": "Failed to list secrets",
                    "error": str(e)
                }]
        
        @self.mcp.tool()
        async def delete_secret(
            name: str,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a secret.
            
            Warning: This operation deletes the secret and may break applications using it.
            
            Args:
                name: Name of the secret to delete
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting secret: {name}")
            
            try:
                secrets_api = hopsworks.get_secrets_api()
                secret = secrets_api.get_secret(name=name)
                
                if not secret:
                    return {
                        "status": "not_found",
                        "message": f"Secret not found: {name}"
                    }
                
                secret.delete()
                
                return {
                    "name": name,
                    "status": "deleted",
                    "message": f"Secret '{name}' deleted successfully"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete secret: {name}",
                    "error": str(e)
                }