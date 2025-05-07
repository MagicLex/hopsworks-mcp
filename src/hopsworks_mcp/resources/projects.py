"""Project resources for Hopsworks."""

import hopsworks


class ProjectResources:
    """Resources for Hopsworks projects."""

    def __init__(self, mcp):
        """Initialize project resources.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        # Register resources
        self.mcp.resource("hopsworks://projects")(self.list_projects)
        self.mcp.resource("hopsworks://projects/{project_id}")(self.get_project)

    def list_projects(self):
        """List all Hopsworks projects."""
        try:
            # Note: This is not directly available in the Hopsworks Python client
            # For now, we'll return a list with just the current project
            project = hopsworks.get_current_project()
            
            return [{
                "name": project.name,
                "id": project.id,
                "owner": project.owner,
                "description": project.description,
                "created": project.created,
                "project_namespace": project.project_namespace
            }]
        except Exception:
            return []

    def get_project(self, project_id: int):
        """Get a specific Hopsworks project.
        
        Args:
            project_id: ID of the project to retrieve
        """
        try:
            # Currently the Hopsworks Python client doesn't support
            # fetching a project by ID, so we check if the current project
            # matches the requested ID
            current_project = hopsworks.get_current_project()
            
            if current_project.id == project_id:
                return {
                    "name": current_project.name,
                    "id": current_project.id,
                    "owner": current_project.owner,
                    "description": current_project.description,
                    "created": current_project.created,
                    "project_namespace": current_project.project_namespace
                }
            
            return {
                "status": "error",
                "message": f"Project with ID {project_id} not found or not accessible"
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error retrieving project: {str(e)}"
            }