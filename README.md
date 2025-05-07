# Hopsworks MCP Server

MCP server for Hopsworks integration, providing a straightforward interface for LLMs to interact with Hopsworks.

## Capabilities

- **Authentication** - Connect to Hopsworks instances
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
- **Model Registry** - Create, save, retrieve and manage ML models (TensorFlow, PyTorch, scikit-learn, Python, LLM)
- **Model Serving** - Deploy, manage and monitor ML models in production with advanced features like transformers, inference logging and batching
- **Projects** - Create and manage Hopsworks projects
- **Datasets** - Handle file operations on Hopsworks
- **Python Environments** - Manage Python environments and dependencies
- **Jobs** - Create and schedule jobs
- **Executions** - Run and monitor job executions
- **Flink Clusters** - Manage Flink clusters and jobs
- **Git Integration** - Work with Git repositories within Hopsworks
- **Kafka** - Create and manage Kafka topics and schemas
- **OpenSearch** - Work with OpenSearch indexes
- **Secrets** - Securely store and retrieve sensitive information

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
- Hopsworks API access (API key)
