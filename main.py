"""Main entry point for the Hopsworks MCP server."""

from src.hopsworks_mcp.server import mcp
from src.hopsworks_mcp.tools.auth import AuthTools
from src.hopsworks_mcp.tools.feature_store import FeatureStoreTools
from src.hopsworks_mcp.tools.model_registry import ModelRegistryTools
from src.hopsworks_mcp.tools.model_serving import ModelServingTools
from src.hopsworks_mcp.tools.projects import ProjectTools
from src.hopsworks_mcp.tools.datasets import DatasetTools
from src.hopsworks_mcp.tools.environments import EnvironmentTools
from src.hopsworks_mcp.tools.executions import ExecutionTools
from src.hopsworks_mcp.tools.flink import FlinkTools
from src.hopsworks_mcp.resources.projects import ProjectResources

# Initialize tools and resources
auth = AuthTools(mcp)
feature_store = FeatureStoreTools(mcp)
model_registry = ModelRegistryTools(mcp)
model_serving = ModelServingTools(mcp)
projects = ProjectTools(mcp)
datasets = DatasetTools(mcp)
environments = EnvironmentTools(mcp)
executions = ExecutionTools(mcp)
flink = FlinkTools(mcp)
project_resources = ProjectResources(mcp)

if __name__ == "__main__":
    mcp.run()