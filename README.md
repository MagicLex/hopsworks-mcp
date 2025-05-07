# Hopsworks MCP Server

MCP server for Hopsworks integration, providing a straightforward interface for LLMs to interact with Hopsworks.

## Capabilities

### Platform & Authentication
- **Authentication** - Connect to Hopsworks instances
- **Projects** - Create and manage Hopsworks projects
- **Datasets** - Handle file operations on Hopsworks
- **Python Environments** - Manage Python environments and dependencies
- **Secrets** - Securely store and retrieve sensitive information

### Feature Store
- **Feature Store** - Interact with feature stores and run SQL queries
- **Feature Groups** - Manage feature groups and their data
- **External Feature Groups** - Connect to external data sources as feature groups
- **Features** - Work with individual features and their metadata
- **Feature Views** - Create and use feature views for model training and serving 
- **Expectations** - Create and manage data validation rules
- **Embeddings** - Manage vector embeddings and similarity search
- **Queries** - Join, filter, and analyze feature data
- **Spine Groups** - Create and use spine groups for training data generation
- **Training Datasets** - Create and manage datasets for model training
- **Transformation Functions** - Create and manage feature transformation functions

### Model Lifecycle
- **Model Registry** - Create, save, retrieve and manage ML models (TensorFlow, PyTorch, scikit-learn, Python, LLM)
- **Model Serving** - Deploy, manage and monitor ML models in production with advanced features like transformers, inference logging and batching

### Jobs & Processing
- **Jobs** - Create and schedule jobs
- **Executions** - Run and monitor job executions
- **Flink Clusters** - Manage Flink clusters and jobs

### Integrations
- **Git Integration** - Work with Git repositories within Hopsworks
- **Kafka** - Create and manage Kafka topics and schemas
- **OpenSearch** - Work with OpenSearch indexes

## Installation

```bash
pip install -e .
```

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run the server
fastmcp run main.py

# Use the interactive development environment
fastmcp dev main.py
```

## Usage with Claude or other LLMs

Once the server is running, you can use it with any MCP-compatible LLM client:

```bash
# Install in Claude Desktop for persistent access
fastmcp install main.py --name "Hopsworks Tools"
```

## Requirements

- Python 3.10+
- Hopsworks API access (API key with recommended scopes: featurestore, project, job, kafka)

## Best Practices

### Installation
- The Hopsworks Python client is installed with the Python profile (`hopsworks[python]`) to ensure all necessary dependencies are available for pure Python environments.
- For Spark environments, refer to the [Spark integration guide](https://docs.hopsworks.ai/latest/integrations/spark/) for proper configuration.

### API Key
- When generating an API key, include the following scopes: `featurestore`, `project`, `job`, and `kafka` for full functionality.
- Store API keys securely and never commit them to version control.

### Engine Selection
- Use the appropriate engine based on your environment:
  - `python`: For pure Python environments (default)
  - `spark`: For Apache Spark environments
  - `hive`: For Hive query execution

### Version Compatibility
- The major and minor version of the Hopsworks Python library should match those of your Hopsworks deployment.
- Check your Hopsworks version in the Project's settings tab.