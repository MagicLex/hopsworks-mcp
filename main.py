"""Main entry point for the Hopsworks MCP server."""

from src.hopsworks_mcp.server import mcp
from src.hopsworks_mcp.tools.feature_store import FeatureStoreTools
from src.hopsworks_mcp.resources.projects import ProjectResources

# Initialize tools and resources
feature_store = FeatureStoreTools(mcp)
projects = ProjectResources(mcp)

if __name__ == "__main__":
    mcp.run()