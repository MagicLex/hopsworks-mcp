"""Data models for Hopsworks MCP."""

from pydantic import BaseModel
from typing import Optional, Dict, Any, List


class Connection(BaseModel):
    """Hopsworks connection information."""
    
    host: Optional[str] = None
    port: int = 443
    project: Optional[str] = None
    api_key: Optional[str] = None
    connected: bool = False
    
    # References to Hopsworks objects (filled after successful connection)
    project_ref: Optional[Dict[str, Any]] = None
    feature_store_ref: Optional[Dict[str, Any]] = None
    model_registry_ref: Optional[Dict[str, Any]] = None
    model_serving_ref: Optional[Dict[str, Any]] = None


class FeatureGroup(BaseModel):
    """Hopsworks Feature Group information."""
    
    name: str
    version: int
    description: Optional[str] = None
    features: Optional[List[Dict[str, Any]]] = None


class Model(BaseModel):
    """Hopsworks Model information."""
    
    name: str
    version: int
    description: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None