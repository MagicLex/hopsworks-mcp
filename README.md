# Hopsworks MCP Server

MCP server for Hopsworks integration, providing a straightforward interface for LLMs to interact with Hopsworks features.

## Features

- **Authentication** - Connect to Hopsworks instances
- **Feature Store** - Interact with feature groups and feature views
- **Model Registry** - Manage ML models
- **Model Serving** - Deploy models to production
- **Datasets** - Handle file operations on Hopsworks
- **Python Environments** - Manage Python environments and dependencies
- **Executions** - Run and monitor jobs

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