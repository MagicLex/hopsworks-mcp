"""Model registry tools for Hopsworks."""

from fastmcp import Context
from typing import List, Dict, Any, Optional, Union, Literal
import hopsworks
import json
import tempfile
import os
import base64


class ModelRegistryTools:
    """Tools for interacting with Hopsworks Model Registry."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        @self.mcp.tool()
        async def get_model_registry(
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Connect to project's Model Registry.
            
            Returns:
                Model registry information
            """
            if ctx:
                await ctx.info("Getting model registry for current project")
            
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                return {
                    "id": mr.model_registry_id,
                    "project_name": mr.project_name,
                    "project_id": mr.project_id,
                    "project_path": mr.project_path,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to connect to model registry: {str(e)}"
                }
        
        @self.mcp.tool()
        async def list_models(
            model_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List models in the Model Registry.
            
            Args:
                model_name: Optional filter by model name
                
            Returns:
                List of models
            """
            if ctx:
                if model_name:
                    await ctx.info(f"Listing versions of model '{model_name}'")
                else:
                    await ctx.info("Listing all models in the registry")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                result = []
                
                if model_name:
                    # Get all versions of a specific model
                    models = mr.get_models(name=model_name)
                    for model in models:
                        result.append({
                            "name": model.name,
                            "version": model.version,
                            "description": model.description,
                            "created": str(model.created) if hasattr(model, "created") else None,
                            "framework": model.framework if hasattr(model, "framework") else None,
                            "status": "success"
                        })
                else:
                    # Get all models (first version only)
                    all_models = []
                    # Unfortunately, it seems we need to query the API for all models of a given name
                    # Let's try to get a list of unique model names first
                    model_names = set()
                    models = mr.get_models()
                    
                    for model in models:
                        if model.name not in model_names:
                            model_names.add(model.name)
                            all_models.append(model)
                    
                    for model in all_models:
                        result.append({
                            "name": model.name,
                            "version": model.version,
                            "description": model.description,
                            "created": str(model.created) if hasattr(model, "created") else None,
                            "framework": model.framework if hasattr(model, "framework") else None,
                            "status": "success"
                        })
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list models: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def get_model(
            name: str,
            version: Optional[int] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a specific model from the registry.
            
            Args:
                name: Name of the model
                version: Version of the model (if None, gets the latest)
                
            Returns:
                Model details
            """
            version_info = f"(v{version})" if version else "(latest)"
            if ctx:
                await ctx.info(f"Getting model {name} {version_info}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Extract metrics in a serializable format
                metrics = {}
                if hasattr(model, "training_metrics") and model.training_metrics:
                    metrics = model.training_metrics
                
                model_info = {
                    "name": model.name,
                    "version": model.version,
                    "description": model.description,
                    "framework": model.framework if hasattr(model, "framework") else None,
                    "created": str(model.created) if hasattr(model, "created") else None,
                    "creator": model.creator if hasattr(model, "creator") else None,
                    "metrics": metrics,
                    "model_path": model.model_path if hasattr(model, "model_path") else None,
                    "version_path": model.version_path if hasattr(model, "version_path") else None,
                    "status": "success"
                }
                
                # Add model schema if available
                if hasattr(model, "model_schema") and model.model_schema:
                    schema_info = {}
                    
                    # Add input schema
                    if hasattr(model.model_schema, "input_schema") and model.model_schema.input_schema:
                        input_schema = []
                        for col in model.model_schema.input_schema.fields:
                            input_schema.append({
                                "name": col.name,
                                "type": col.type,
                                "description": col.description
                            })
                        schema_info["input_schema"] = input_schema
                    
                    # Add output schema
                    if hasattr(model.model_schema, "output_schema") and model.model_schema.output_schema:
                        output_schema = []
                        for col in model.model_schema.output_schema.fields:
                            output_schema.append({
                                "name": col.name,
                                "type": col.type,
                                "description": col.description
                            })
                        schema_info["output_schema"] = output_schema
                    
                    model_info["schema"] = schema_info
                
                return model_info
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_best_model(
            name: str,
            metric: str,
            direction: Literal["max", "min"],
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the best performing model based on a metric.
            
            Args:
                name: Name of the model
                metric: Name of the metric to compare (e.g., 'accuracy', 'loss')
                direction: Whether to find the maximum ('max') or minimum ('min') value
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Getting best model {name} based on {metric} ({direction})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_best_model(name=name, metric=metric, direction=direction)
                
                # Extract metrics in a serializable format
                metrics = {}
                if hasattr(model, "training_metrics") and model.training_metrics:
                    metrics = model.training_metrics
                
                return {
                    "name": model.name,
                    "version": model.version,
                    "description": model.description,
                    "framework": model.framework if hasattr(model, "framework") else None,
                    "created": str(model.created) if hasattr(model, "created") else None,
                    "metrics": metrics,
                    "best_metric": {
                        "name": metric,
                        "value": metrics.get(metric),
                        "direction": direction
                    },
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get best model: {str(e)}"
                }
                
        @self.mcp.tool()
        async def create_tensorflow_model(
            name: str,
            model_path: str,
            version: Optional[int] = None,
            metrics: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None,
            input_example: Optional[Dict[str, Any]] = None,
            await_registration: int = 480,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create and save a TensorFlow model in the model registry.
            
            Args:
                name: Name of the model
                model_path: Path to the model directory or file
                version: Version of the model (if None, auto-increments)
                metrics: Dictionary of model evaluation metrics (e.g., {'accuracy': 0.95})
                description: Description of the model
                input_example: Example input data for the model
                await_registration: Time in seconds to wait for model registration
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Creating TensorFlow model {name}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                # Create the model metadata object
                model = mr.tensorflow.create_model(
                    name=name,
                    version=version,
                    metrics=metrics,
                    description=description
                )
                
                # Save the model, which will upload the model files
                saved_model = model.save(
                    model_path=model_path,
                    await_registration=await_registration
                )
                
                return {
                    "name": saved_model.name,
                    "version": saved_model.version,
                    "description": saved_model.description,
                    "framework": "tensorflow",
                    "metrics": metrics,
                    "model_path": saved_model.model_path if hasattr(saved_model, "model_path") else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create TensorFlow model: {str(e)}"
                }
                
        @self.mcp.tool()
        async def create_pytorch_model(
            name: str,
            model_path: str,
            version: Optional[int] = None,
            metrics: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None,
            input_example: Optional[Dict[str, Any]] = None,
            await_registration: int = 480,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create and save a PyTorch model in the model registry.
            
            Args:
                name: Name of the model
                model_path: Path to the model directory or file
                version: Version of the model (if None, auto-increments)
                metrics: Dictionary of model evaluation metrics (e.g., {'accuracy': 0.95})
                description: Description of the model
                input_example: Example input data for the model
                await_registration: Time in seconds to wait for model registration
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Creating PyTorch model {name}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                # Create the model metadata object
                model = mr.torch.create_model(
                    name=name,
                    version=version,
                    metrics=metrics,
                    description=description
                )
                
                # Save the model, which will upload the model files
                saved_model = model.save(
                    model_path=model_path,
                    await_registration=await_registration
                )
                
                return {
                    "name": saved_model.name,
                    "version": saved_model.version,
                    "description": saved_model.description,
                    "framework": "pytorch",
                    "metrics": metrics,
                    "model_path": saved_model.model_path if hasattr(saved_model, "model_path") else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create PyTorch model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_sklearn_model(
            name: str,
            model_path: str,
            version: Optional[int] = None,
            metrics: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None,
            input_example: Optional[Dict[str, Any]] = None,
            await_registration: int = 480,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create and save a scikit-learn model in the model registry.
            
            Args:
                name: Name of the model
                model_path: Path to the model directory or file
                version: Version of the model (if None, auto-increments)
                metrics: Dictionary of model evaluation metrics (e.g., {'accuracy': 0.95})
                description: Description of the model
                input_example: Example input data for the model
                await_registration: Time in seconds to wait for model registration
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Creating scikit-learn model {name}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                # Create the model metadata object
                model = mr.sklearn.create_model(
                    name=name,
                    version=version,
                    metrics=metrics,
                    description=description
                )
                
                # Save the model, which will upload the model files
                saved_model = model.save(
                    model_path=model_path,
                    await_registration=await_registration
                )
                
                return {
                    "name": saved_model.name,
                    "version": saved_model.version,
                    "description": saved_model.description,
                    "framework": "sklearn",
                    "metrics": metrics,
                    "model_path": saved_model.model_path if hasattr(saved_model, "model_path") else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create scikit-learn model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_python_model(
            name: str,
            model_path: str,
            version: Optional[int] = None,
            metrics: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None,
            input_example: Optional[Dict[str, Any]] = None,
            await_registration: int = 480,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create and save a Python model in the model registry.
            
            Args:
                name: Name of the model
                model_path: Path to the model directory or file
                version: Version of the model (if None, auto-increments)
                metrics: Dictionary of model evaluation metrics (e.g., {'accuracy': 0.95})
                description: Description of the model
                input_example: Example input data for the model
                await_registration: Time in seconds to wait for model registration
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Creating Python model {name}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                # Create the model metadata object
                model = mr.python.create_model(
                    name=name,
                    version=version,
                    metrics=metrics,
                    description=description
                )
                
                # Save the model, which will upload the model files
                saved_model = model.save(
                    model_path=model_path,
                    await_registration=await_registration
                )
                
                return {
                    "name": saved_model.name,
                    "version": saved_model.version,
                    "description": saved_model.description,
                    "framework": "python",
                    "metrics": metrics,
                    "model_path": saved_model.model_path if hasattr(saved_model, "model_path") else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create Python model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_llm_model(
            name: str,
            model_path: str,
            version: Optional[int] = None,
            metrics: Optional[Dict[str, Any]] = None,
            description: Optional[str] = None,
            input_example: Optional[Dict[str, Any]] = None,
            await_registration: int = 480,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create and save a Large Language Model (LLM) in the model registry.
            
            Args:
                name: Name of the model
                model_path: Path to the model directory or file
                version: Version of the model (if None, auto-increments)
                metrics: Dictionary of model evaluation metrics (e.g., {'perplexity': 5.2})
                description: Description of the model
                input_example: Example input data for the model
                await_registration: Time in seconds to wait for model registration
                
            Returns:
                Model details
            """
            if ctx:
                await ctx.info(f"Creating LLM model {name}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                
                # Create the model metadata object
                model = mr.llm.create_model(
                    name=name,
                    version=version,
                    metrics=metrics,
                    description=description
                )
                
                # Save the model, which will upload the model files
                saved_model = model.save(
                    model_path=model_path,
                    await_registration=await_registration
                )
                
                return {
                    "name": saved_model.name,
                    "version": saved_model.version,
                    "description": saved_model.description,
                    "framework": "llm",
                    "metrics": metrics,
                    "model_path": saved_model.model_path if hasattr(saved_model, "model_path") else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create LLM model: {str(e)}"
                }
                
        @self.mcp.tool()
        async def download_model(
            name: str,
            version: Optional[int] = None,
            destination_path: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Download a model from the registry.
            
            Args:
                name: Name of the model
                version: Version of the model (if None, gets the latest)
                destination_path: Local path to download the model to (if None, uses a temporary directory)
                
            Returns:
                Download information
            """
            version_info = f"(v{version})" if version else "(latest)"
            if ctx:
                await ctx.info(f"Downloading model {name} {version_info}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # If no destination is provided, use a temporary directory
                download_path = destination_path
                if not download_path:
                    download_path = tempfile.mkdtemp(prefix=f"model_{name}_v{model.version}_")
                
                # Download the model
                local_path = model.download(local_path=download_path)
                
                return {
                    "name": model.name,
                    "version": model.version,
                    "framework": model.framework if hasattr(model, "framework") else None,
                    "local_path": local_path,
                    "status": "downloaded"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to download model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_model(
            name: str,
            version: int,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a model from the registry.
            
            Args:
                name: Name of the model
                version: Version of the model (required for deletion)
                
            Returns:
                Deletion status
            """
            if ctx:
                await ctx.info(f"Deleting model {name} (v{version})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Delete the model
                model.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete model: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_model_schema(
            name: str,
            version: Optional[int] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the schema information for a model.
            
            Args:
                name: Name of the model
                version: Version of the model (if None, gets the latest)
                
            Returns:
                Model schema details
            """
            version_info = f"(v{version})" if version else "(latest)"
            if ctx:
                await ctx.info(f"Getting schema for model {name} {version_info}")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                if not hasattr(model, "model_schema") or not model.model_schema:
                    return {
                        "name": model.name,
                        "version": model.version,
                        "status": "error",
                        "message": "Model does not have a schema defined"
                    }
                
                schema_info = {}
                
                # Add input schema
                if hasattr(model.model_schema, "input_schema") and model.model_schema.input_schema:
                    input_schema = []
                    for col in model.model_schema.input_schema.fields:
                        input_schema.append({
                            "name": col.name,
                            "type": col.type,
                            "description": col.description
                        })
                    schema_info["input_schema"] = input_schema
                
                # Add output schema
                if hasattr(model.model_schema, "output_schema") and model.model_schema.output_schema:
                    output_schema = []
                    for col in model.model_schema.output_schema.fields:
                        output_schema.append({
                            "name": col.name,
                            "type": col.type,
                            "description": col.description
                        })
                    schema_info["output_schema"] = output_schema
                
                return {
                    "name": model.name,
                    "version": model.version,
                    "schema": schema_info,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get model schema: {str(e)}"
                }
                
        @self.mcp.tool()
        async def set_model_tag(
            name: str,
            version: int,
            tag_name: str,
            tag_value: Union[str, Dict[str, Any], List[Any]],
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Set a tag on a model.
            
            Args:
                name: Name of the model
                version: Version of the model
                tag_name: Name of the tag
                tag_value: Value of the tag (string, dict, or list)
                
            Returns:
                Tag status
            """
            if ctx:
                await ctx.info(f"Setting tag '{tag_name}' on model {name} (v{version})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Set the tag
                model.set_tag(name=tag_name, value=tag_value)
                
                return {
                    "name": name,
                    "version": version,
                    "tag": {
                        "name": tag_name,
                        "value": tag_value
                    },
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to set tag: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_model_tags(
            name: str,
            version: int,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get all tags for a model.
            
            Args:
                name: Name of the model
                version: Version of the model
                
            Returns:
                Model tags
            """
            if ctx:
                await ctx.info(f"Getting tags for model {name} (v{version})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Get all tags
                tags = model.get_tags()
                
                return {
                    "name": name,
                    "version": version,
                    "tags": tags,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get tags: {str(e)}"
                }
                
        @self.mcp.tool()
        async def delete_model_tag(
            name: str,
            version: int,
            tag_name: str,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a tag from a model.
            
            Args:
                name: Name of the model
                version: Version of the model
                tag_name: Name of the tag to delete
                
            Returns:
                Deletion status
            """
            if ctx:
                await ctx.info(f"Deleting tag '{tag_name}' from model {name} (v{version})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Delete the tag
                model.delete_tag(name=tag_name)
                
                return {
                    "name": name,
                    "version": version,
                    "tag_name": tag_name,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete tag: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_model_url(
            name: str,
            version: int,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the URL for a model in the Hopsworks UI.
            
            Args:
                name: Name of the model
                version: Version of the model
                
            Returns:
                URL information
            """
            if ctx:
                await ctx.info(f"Getting URL for model {name} (v{version})")
                
            try:
                project = hopsworks.get_current_project()
                mr = project.get_model_registry()
                model = mr.get_model(name=name, version=version)
                
                # Get the URL
                url = model.get_url()
                
                return {
                    "name": name,
                    "version": version,
                    "url": url,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get model URL: {str(e)}"
                }