"""MCP server for Hopsworks."""

from fastmcp import FastMCP

# Create a FastMCP server instance
mcp = FastMCP(name="Hopsworks MCP")


if __name__ == "__main__":
    mcp.run()
