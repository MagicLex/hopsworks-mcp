"""Main entry point for the Hopsworks MCP server."""

from src.hopsworks_mcp.server import mcp
from src.hopsworks_mcp.tools.auth import AuthTools
from src.hopsworks_mcp.tools.feature_store import FeatureStoreTools
from src.hopsworks_mcp.tools.feature_groups import FeatureGroupTools
from src.hopsworks_mcp.tools.feature_views import FeatureViewTools
from src.hopsworks_mcp.tools.external_feature_groups import ExternalFeatureGroupTools
from src.hopsworks_mcp.tools.features import FeatureTools
from src.hopsworks_mcp.tools.expectations import ExpectationTools
from src.hopsworks_mcp.tools.embeddings import EmbeddingTools
from src.hopsworks_mcp.tools.queries import QueryTools
from src.hopsworks_mcp.tools.spine_groups import SpineGroupTools
from src.hopsworks_mcp.tools.training_datasets import TrainingDatasetTools
from src.hopsworks_mcp.tools.model_registry import ModelRegistryTools
from src.hopsworks_mcp.tools.model_serving import ModelServingTools
from src.hopsworks_mcp.tools.projects import ProjectTools
from src.hopsworks_mcp.tools.datasets import DatasetTools
from src.hopsworks_mcp.tools.environments import EnvironmentTools
from src.hopsworks_mcp.tools.executions import ExecutionTools
from src.hopsworks_mcp.tools.flink import FlinkTools
from src.hopsworks_mcp.tools.git import GitTools
from src.hopsworks_mcp.tools.jobs import JobTools
from src.hopsworks_mcp.tools.kafka import KafkaTools
from src.hopsworks_mcp.tools.opensearch import OpenSearchTools
from src.hopsworks_mcp.tools.secrets import SecretsTools
from src.hopsworks_mcp.resources.projects import ProjectResources

# Initialize tools and resources
auth = AuthTools(mcp)
feature_store = FeatureStoreTools(mcp)
feature_groups = FeatureGroupTools(mcp)
feature_views = FeatureViewTools(mcp)
external_feature_groups = ExternalFeatureGroupTools(mcp)
features = FeatureTools(mcp)
expectations = ExpectationTools(mcp)
embeddings = EmbeddingTools(mcp)
queries = QueryTools(mcp)
spine_groups = SpineGroupTools(mcp)
training_datasets = TrainingDatasetTools(mcp)
model_registry = ModelRegistryTools(mcp)
model_serving = ModelServingTools(mcp)
projects = ProjectTools(mcp)
datasets = DatasetTools(mcp)
environments = EnvironmentTools(mcp)
executions = ExecutionTools(mcp)
flink = FlinkTools(mcp)
git = GitTools(mcp)
jobs = JobTools(mcp)
kafka = KafkaTools(mcp)
opensearch = OpenSearchTools(mcp)
secrets = SecretsTools(mcp)
project_resources = ProjectResources(mcp)

if __name__ == "__main__":
    mcp.run()