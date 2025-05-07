"""Project resources for Hopsworks."""


class ProjectResources:
    """Resources for Hopsworks projects."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register resources
        self.mcp.resource("hopsworks://projects")(self.list_projects)
        self.mcp.resource("hopsworks://projects/{project_id}")(self.get_project)

    def list_projects(self):
        """List all Hopsworks projects."""
        return []

    def get_project(self, project_id: int):
        """Get a specific Hopsworks project.
        
        Args:
            project_id: ID of the project to retrieve
        """
        return {}