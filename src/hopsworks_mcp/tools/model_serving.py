"""Model serving tools for Hopsworks."""

from fastmcp import Context
from typing import List, Dict, Any, Optional, Union, Literal
import hopsworks
import json
import os
import tempfile


class ModelServingTools:
    """Tools for interacting with Hopsworks Model Serving."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        @self.mcp.tool()
        async def get_model_serving(
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Connect to project's Model Serving.
            
            Returns:
                Model serving information
            """
            if ctx:
                await ctx.info("Getting model serving for current project")
            
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                
                return {
                    "project_id": ms.project_id,
                    "project_name": ms.project_name,
                    "project_path": ms.project_path if hasattr(ms, "project_path") else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get model serving: {str(e)}"
                }
        
        @self.mcp.tool()
        async def list_deployments(
            model_name: Optional[str] = None,
            status: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List deployed models.
            
            Args:
                model_name: Optional filter by model name
                status: Optional filter by deployment status
                
            Returns:
                List of deployments
            """
            if ctx:
                filter_msg = []
                if model_name:
                    filter_msg.append(f"model '{model_name}'")
                if status:
                    filter_msg.append(f"status '{status}'")
                
                if filter_msg:
                    await ctx.info(f"Listing model deployments filtered by {' and '.join(filter_msg)}")
                else:
                    await ctx.info("Listing all model deployments")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                
                # Get the model if filtering by model name
                model = None
                if model_name:
                    mr = project.get_model_registry()
                    model = mr.get_model(name=model_name)
                
                deployments = ms.get_deployments(model=model, status=status)
                
                result = []
                for deployment in deployments:
                    deployment_state = deployment.get_state()
                    result.append({
                        "name": deployment.name,
                        "id": deployment.id if hasattr(deployment, "id") else None,
                        "model_name": deployment.model_name if hasattr(deployment, "model_name") else None,
                        "model_version": deployment.model_version if hasattr(deployment, "model_version") else None,
                        "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                        "created": str(deployment.created_at) if hasattr(deployment, "created_at") else None,
                        "framework": deployment.model_server if hasattr(deployment, "model_server") else None,
                        "api_protocol": deployment.api_protocol if hasattr(deployment, "api_protocol") else None,
                        "instances": deployment.requested_instances if hasattr(deployment, "requested_instances") else None,
                        "status": "success"
                    })
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list deployments: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def get_deployment(
            name: str,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a specific deployment by name.
            
            Args:
                name: Name of the deployment
                
            Returns:
                Deployment details
            """
            if ctx:
                await ctx.info(f"Getting deployment: {name}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                deployment_state = deployment.get_state()
                
                # Get the predictor resources if available
                predictor_resources = {}
                if hasattr(deployment, "resources") and deployment.resources:
                    predictor_resources = {
                        "cores": deployment.resources.cores,
                        "memory": deployment.resources.memory,
                        "gpus": deployment.resources.gpus
                    }
                
                # Get transformer details if available
                transformer_info = None
                if hasattr(deployment, "transformer") and deployment.transformer:
                    transformer_resources = {}
                    if hasattr(deployment.transformer, "resources") and deployment.transformer.resources:
                        transformer_resources = {
                            "cores": deployment.transformer.resources.cores,
                            "memory": deployment.transformer.resources.memory,
                            "gpus": deployment.transformer.resources.gpus
                        }
                    
                    transformer_info = {
                        "script_file": deployment.transformer.script_file if hasattr(deployment.transformer, "script_file") else None,
                        "resources": transformer_resources
                    }
                
                # Get inference logger details if available
                inference_logger = None
                if hasattr(deployment, "inference_logger") and deployment.inference_logger:
                    inference_logger = {
                        "mode": deployment.inference_logger.mode if hasattr(deployment.inference_logger, "mode") else None,
                        "kafka_topic": deployment.inference_logger.kafka_topic if hasattr(deployment.inference_logger, "kafka_topic") else None
                    }
                
                # Get inference batcher details if available
                inference_batcher = None
                if hasattr(deployment, "inference_batcher") and deployment.inference_batcher:
                    inference_batcher = {
                        "enabled": deployment.inference_batcher.enabled if hasattr(deployment.inference_batcher, "enabled") else False,
                        "max_batch_size": deployment.inference_batcher.max_batch_size if hasattr(deployment.inference_batcher, "max_batch_size") else None,
                        "max_latency": deployment.inference_batcher.max_latency if hasattr(deployment.inference_batcher, "max_latency") else None,
                        "timeout": deployment.inference_batcher.timeout if hasattr(deployment.inference_batcher, "timeout") else None
                    }
                
                return {
                    "name": deployment.name,
                    "id": deployment.id if hasattr(deployment, "id") else None,
                    "model_name": deployment.model_name if hasattr(deployment, "model_name") else None,
                    "model_version": deployment.model_version if hasattr(deployment, "model_version") else None,
                    "artifact_version": deployment.artifact_version if hasattr(deployment, "artifact_version") else None,
                    "serving_tool": deployment.serving_tool if hasattr(deployment, "serving_tool") else None,
                    "model_server": deployment.model_server if hasattr(deployment, "model_server") else None,
                    "script_file": deployment.script_file if hasattr(deployment, "script_file") else None,
                    "config_file": deployment.config_file if hasattr(deployment, "config_file") else None,
                    "api_protocol": deployment.api_protocol if hasattr(deployment, "api_protocol") else None,
                    "requested_instances": deployment.requested_instances if hasattr(deployment, "requested_instances") else None,
                    "resources": predictor_resources,
                    "transformer": transformer_info,
                    "inference_logger": inference_logger,
                    "inference_batcher": inference_batcher,
                    "state": {
                        "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                        "available_predictor_instances": deployment_state.available_predictor_instances if hasattr(deployment_state, "available_predictor_instances") else None,
                        "available_transformer_instances": deployment_state.available_transformer_instances if hasattr(deployment_state, "available_transformer_instances") else None,
                        "condition": {
                            "type": deployment_state.condition.type if hasattr(deployment_state, "condition") and hasattr(deployment_state.condition, "type") else None,
                            "status": deployment_state.condition.status if hasattr(deployment_state, "condition") and hasattr(deployment_state.condition, "status") else None,
                            "reason": deployment_state.condition.reason if hasattr(deployment_state, "condition") and hasattr(deployment_state.condition, "reason") else None
                        }
                    },
                    "created_at": str(deployment.created_at) if hasattr(deployment, "created_at") else None,
                    "creator": deployment.creator if hasattr(deployment, "creator") else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get deployment: {str(e)}"
                }
        
        @self.mcp.tool()
        async def deploy_model(
            model_name: str,
            model_version: int,
            deployment_name: Optional[str] = None,
            num_instances: int = 1,
            api_protocol: Literal["REST", "GRPC"] = "REST",
            cpu_cores: float = 1.0,
            memory_mb: int = 1024,
            gpu_count: int = 0,
            script_file: Optional[str] = None,
            config_file: Optional[str] = None,
            enable_logging: bool = False,
            logging_mode: Literal["ALL", "PREDICTIONS", "MODEL_INPUTS", "NONE"] = "ALL",
            enable_batching: bool = False,
            max_batch_size: Optional[int] = None,
            max_batch_latency: Optional[int] = None,
            description: Optional[str] = None,
            artifact_version: Literal["CREATE", "MODEL-ONLY"] = "CREATE",
            environment: Optional[str] = None,
            await_running: Optional[int] = 600,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Deploy a model to the serving infrastructure.
            
            Args:
                model_name: Name of the model to deploy
                model_version: Version of the model to deploy
                deployment_name: Name for the deployment (defaults to model name)
                num_instances: Number of serving instances to deploy
                api_protocol: API protocol (REST or GRPC)
                cpu_cores: Number of CPU cores per instance
                memory_mb: Memory in MB per instance
                gpu_count: Number of GPUs per instance
                script_file: Path to custom predictor implementation
                config_file: Path to model server configuration
                enable_logging: Enable inference logging
                logging_mode: Mode for inference logging (ALL, PREDICTIONS, MODEL_INPUTS, NONE)
                enable_batching: Enable inference batching
                max_batch_size: Maximum batch size for inference
                max_batch_latency: Maximum latency for batch processing (ms)
                description: Description for the deployment
                artifact_version: Version strategy for the model artifact
                environment: Name of the inference environment to use
                await_running: Time in seconds to wait for deployment to start
                
            Returns:
                Deployment information
            """
            serving_name = deployment_name or model_name
            
            if ctx:
                await ctx.info(f"Deploying model {model_name} (v{model_version}) as '{serving_name}'")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                mr = project.get_model_registry()
                
                # Get the model
                model = mr.get_model(name=model_name, version=model_version)
                
                # Configure resources
                resources = {
                    "cores": cpu_cores,
                    "memory": memory_mb,
                    "gpus": gpu_count
                }
                
                # Configure inference logger if enabled
                inference_logger = None
                if enable_logging:
                    inference_logger = {
                        "mode": logging_mode
                    }
                
                # Configure inference batcher if enabled
                inference_batcher = None
                if enable_batching:
                    inference_batcher = {
                        "enabled": True,
                        "max_batch_size": max_batch_size,
                        "max_latency": max_batch_latency
                    }
                
                # Deploy the model directly
                deployment = model.deploy(
                    name=serving_name,
                    description=description,
                    artifact_version=artifact_version,
                    script_file=script_file,
                    config_file=config_file,
                    resources=resources,
                    inference_logger=inference_logger,
                    inference_batcher=inference_batcher,
                    api_protocol=api_protocol,
                    environment=environment
                )
                
                # Start the deployment if not already started
                if await_running and not deployment.is_running():
                    deployment.start(await_running=await_running)
                
                # Get the current state of deployment
                deployment_state = deployment.get_state()
                
                return {
                    "name": deployment.name,
                    "id": deployment.id if hasattr(deployment, "id") else None,
                    "model": {
                        "name": model.name,
                        "version": model.version
                    },
                    "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                    "endpoint": deployment.endpoint if hasattr(deployment, "endpoint") else None,
                    "requested_instances": deployment.requested_instances if hasattr(deployment, "requested_instances") else num_instances,
                    "available_instances": deployment_state.available_predictor_instances if hasattr(deployment_state, "available_predictor_instances") else None,
                    "api_protocol": deployment.api_protocol if hasattr(deployment, "api_protocol") else api_protocol,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to deploy model: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_predictor(
            model_name: str,
            model_version: int,
            predictor_name: Optional[str] = None,
            api_protocol: Literal["REST", "GRPC"] = "REST",
            cpu_cores: float = 1.0,
            memory_mb: int = 1024,
            gpu_count: int = 0,
            script_file: Optional[str] = None,
            config_file: Optional[str] = None,
            enable_logging: bool = False,
            logging_mode: Literal["ALL", "PREDICTIONS", "MODEL_INPUTS", "NONE"] = "ALL",
            enable_batching: bool = False,
            max_batch_size: Optional[int] = None,
            max_batch_latency: Optional[int] = None,
            artifact_version: Literal["CREATE", "MODEL-ONLY"] = "CREATE",
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a predictor for a model (without deploying it).
            
            Args:
                model_name: Name of the model to use for the predictor
                model_version: Version of the model to use
                predictor_name: Name for the predictor (defaults to model name)
                api_protocol: API protocol (REST or GRPC)
                cpu_cores: Number of CPU cores per instance
                memory_mb: Memory in MB per instance
                gpu_count: Number of GPUs per instance
                script_file: Path to custom predictor implementation
                config_file: Path to model server configuration
                enable_logging: Enable inference logging
                logging_mode: Mode for inference logging (ALL, PREDICTIONS, MODEL_INPUTS, NONE)
                enable_batching: Enable inference batching
                max_batch_size: Maximum batch size for inference
                max_batch_latency: Maximum latency for batch processing (ms)
                artifact_version: Version strategy for the model artifact
                
            Returns:
                Predictor information
            """
            serving_name = predictor_name or model_name
            
            if ctx:
                await ctx.info(f"Creating predictor for model {model_name} (v{model_version}) with name '{serving_name}'")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                mr = project.get_model_registry()
                
                # Get the model
                model = mr.get_model(name=model_name, version=model_version)
                
                # Configure resources
                resources = {
                    "cores": cpu_cores,
                    "memory": memory_mb,
                    "gpus": gpu_count
                }
                
                # Configure inference logger if enabled
                inference_logger = None
                if enable_logging:
                    inference_logger = {
                        "mode": logging_mode
                    }
                
                # Configure inference batcher if enabled
                inference_batcher = None
                if enable_batching:
                    inference_batcher = {
                        "enabled": True,
                        "max_batch_size": max_batch_size,
                        "max_latency": max_batch_latency
                    }
                
                # Create the predictor
                predictor = ms.create_predictor(
                    model=model,
                    name=serving_name,
                    artifact_version=artifact_version,
                    script_file=script_file,
                    config_file=config_file,
                    resources=resources,
                    inference_logger=inference_logger,
                    inference_batcher=inference_batcher,
                    api_protocol=api_protocol
                )
                
                # Get the predictor resources
                predictor_resources = {}
                if hasattr(predictor, "resources") and predictor.resources:
                    predictor_resources = {
                        "cores": predictor.resources.cores,
                        "memory": predictor.resources.memory,
                        "gpus": predictor.resources.gpus
                    }
                
                return {
                    "name": predictor.name,
                    "model_name": predictor.model_name if hasattr(predictor, "model_name") else model_name,
                    "model_version": predictor.model_version if hasattr(predictor, "model_version") else model_version,
                    "model_framework": predictor.model_framework if hasattr(predictor, "model_framework") else None,
                    "artifact_version": predictor.artifact_version if hasattr(predictor, "artifact_version") else artifact_version,
                    "serving_tool": predictor.serving_tool if hasattr(predictor, "serving_tool") else None,
                    "model_server": predictor.model_server if hasattr(predictor, "model_server") else None,
                    "script_file": predictor.script_file if hasattr(predictor, "script_file") else script_file,
                    "config_file": predictor.config_file if hasattr(predictor, "config_file") else config_file,
                    "api_protocol": predictor.api_protocol if hasattr(predictor, "api_protocol") else api_protocol,
                    "resources": predictor_resources,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create predictor: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_and_deploy_predictor(
            model_name: str,
            model_version: int,
            deployment_name: Optional[str] = None,
            num_instances: int = 1,
            api_protocol: Literal["REST", "GRPC"] = "REST",
            cpu_cores: float = 1.0,
            memory_mb: int = 1024,
            gpu_count: int = 0,
            script_file: Optional[str] = None,
            config_file: Optional[str] = None,
            enable_logging: bool = False,
            logging_mode: Literal["ALL", "PREDICTIONS", "MODEL_INPUTS", "NONE"] = "ALL",
            enable_batching: bool = False,
            max_batch_size: Optional[int] = None,
            max_batch_latency: Optional[int] = None,
            description: Optional[str] = None,
            artifact_version: Literal["CREATE", "MODEL-ONLY"] = "CREATE",
            environment: Optional[str] = None,
            await_running: Optional[int] = 600,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a predictor and deploy it in a two-step process.
            
            Args:
                model_name: Name of the model to deploy
                model_version: Version of the model to deploy
                deployment_name: Name for the deployment (defaults to model name)
                num_instances: Number of serving instances to deploy
                api_protocol: API protocol (REST or GRPC)
                cpu_cores: Number of CPU cores per instance
                memory_mb: Memory in MB per instance
                gpu_count: Number of GPUs per instance
                script_file: Path to custom predictor implementation
                config_file: Path to model server configuration
                enable_logging: Enable inference logging
                logging_mode: Mode for inference logging (ALL, PREDICTIONS, MODEL_INPUTS, NONE)
                enable_batching: Enable inference batching
                max_batch_size: Maximum batch size for inference
                max_batch_latency: Maximum latency for batch processing (ms)
                description: Description for the deployment
                artifact_version: Version strategy for the model artifact
                environment: Name of the inference environment to use
                await_running: Time in seconds to wait for deployment to start
                
            Returns:
                Deployment information
            """
            serving_name = deployment_name or model_name
            
            if ctx:
                await ctx.info(f"Creating and deploying predictor for model {model_name} (v{model_version}) as '{serving_name}'")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                mr = project.get_model_registry()
                
                # Get the model
                model = mr.get_model(name=model_name, version=model_version)
                
                # Configure resources
                resources = {
                    "cores": cpu_cores,
                    "memory": memory_mb,
                    "gpus": gpu_count
                }
                
                # Configure inference logger if enabled
                inference_logger = None
                if enable_logging:
                    inference_logger = {
                        "mode": logging_mode
                    }
                
                # Configure inference batcher if enabled
                inference_batcher = None
                if enable_batching:
                    inference_batcher = {
                        "enabled": True,
                        "max_batch_size": max_batch_size,
                        "max_latency": max_batch_latency
                    }
                
                # Create the predictor
                predictor = ms.create_predictor(
                    model=model,
                    name=serving_name,
                    artifact_version=artifact_version,
                    script_file=script_file,
                    config_file=config_file,
                    resources=resources,
                    inference_logger=inference_logger,
                    inference_batcher=inference_batcher,
                    api_protocol=api_protocol
                )
                
                # Create and save the deployment
                deployment = ms.create_deployment(
                    predictor=predictor,
                    name=serving_name,
                    environment=environment
                )
                deployment.save()
                
                # Start the deployment if not already started
                if await_running:
                    deployment.start(await_running=await_running)
                
                # Get the current state of deployment
                deployment_state = deployment.get_state()
                
                return {
                    "name": deployment.name,
                    "id": deployment.id if hasattr(deployment, "id") else None,
                    "model": {
                        "name": model.name,
                        "version": model.version
                    },
                    "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                    "endpoint": deployment.endpoint if hasattr(deployment, "endpoint") else None,
                    "requested_instances": deployment.requested_instances if hasattr(deployment, "requested_instances") else num_instances,
                    "available_instances": deployment_state.available_predictor_instances if hasattr(deployment_state, "available_predictor_instances") else None,
                    "api_protocol": deployment.api_protocol if hasattr(deployment, "api_protocol") else api_protocol,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create and deploy predictor: {str(e)}"
                }
        
        @self.mcp.tool()
        async def start_deployment(
            name: str,
            await_running: int = 600,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Start a deployment.
            
            Args:
                name: Name of the deployment to start
                await_running: Time in seconds to wait for deployment to start
                
            Returns:
                Start operation status
            """
            if ctx:
                await ctx.info(f"Starting deployment: {name}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Start the deployment
                deployment.start(await_running=await_running)
                
                # Get the current state
                deployment_state = deployment.get_state()
                
                return {
                    "name": deployment.name,
                    "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                    "available_instances": deployment_state.available_predictor_instances if hasattr(deployment_state, "available_predictor_instances") else None,
                    "operation": "start",
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to start deployment: {str(e)}"
                }
        
        @self.mcp.tool()
        async def stop_deployment(
            name: str,
            await_stopped: int = 600,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Stop a deployment.
            
            Args:
                name: Name of the deployment to stop
                await_stopped: Time in seconds to wait for deployment to stop
                
            Returns:
                Stop operation status
            """
            if ctx:
                await ctx.info(f"Stopping deployment: {name}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Stop the deployment
                deployment.stop(await_stopped=await_stopped)
                
                # Get the current state
                deployment_state = deployment.get_state()
                
                return {
                    "name": deployment.name,
                    "status": deployment_state.status if hasattr(deployment_state, "status") else None,
                    "operation": "stop",
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to stop deployment: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_deployment(
            name: str,
            force: bool = False,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a deployment.
            
            Args:
                name: Name of the deployment to delete
                force: Force deletion even if deployment is running
                
            Returns:
                Delete operation status
            """
            if ctx:
                await ctx.info(f"Deleting deployment: {name}" + (" (force)" if force else ""))
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Delete the deployment
                deployment.delete(force=force)
                
                return {
                    "name": name,
                    "operation": "delete",
                    "force": force,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete deployment: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_deployment_logs(
            name: str,
            component: Literal["predictor", "transformer"] = "predictor",
            tail: int = 100,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get logs for a deployment.
            
            Args:
                name: Name of the deployment
                component: Component to get logs for (predictor or transformer)
                tail: Number of lines to retrieve from the end of the logs
                
            Returns:
                Deployment logs
            """
            if ctx:
                await ctx.info(f"Getting {component} logs for deployment: {name} (last {tail} lines)")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Get the logs
                logs = deployment.get_logs(component=component, tail=tail)
                
                return {
                    "name": name,
                    "component": component,
                    "logs": logs,
                    "lines": tail,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get deployment logs: {str(e)}"
                }
        
        @self.mcp.tool()
        async def predict(
            name: str,
            data: Dict[str, Any],
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Make a prediction using a deployed model.
            
            Args:
                name: Name of the deployment
                data: Data for prediction (should contain 'instances' key with input data)
                
            Returns:
                Prediction results
            """
            if ctx:
                await ctx.info(f"Making prediction using deployment: {name}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Check if the deployment is running
                if not deployment.is_running():
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' is not running, current state: {deployment.get_state().status}"
                    }
                
                # Make the prediction
                predictions = deployment.predict(data=data)
                
                return {
                    "name": name,
                    "predictions": predictions,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to make prediction: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_transformer(
            script_file: str,
            cpu_cores: float = 0.5,
            memory_mb: int = 512,
            gpu_count: int = 0,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a transformer for preprocessing and postprocessing.
            
            Args:
                script_file: Path to the transformer script implementing preprocess and postprocess methods
                cpu_cores: Number of CPU cores per transformer instance
                memory_mb: Memory in MB per transformer instance
                gpu_count: Number of GPUs per transformer instance
                
            Returns:
                Transformer information
            """
            if ctx:
                await ctx.info(f"Creating transformer with script: {script_file}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                
                # Configure resources
                resources = {
                    "cores": cpu_cores,
                    "memory": memory_mb,
                    "gpus": gpu_count
                }
                
                # Create the transformer
                transformer = ms.create_transformer(
                    script_file=script_file,
                    resources=resources
                )
                
                # Get the transformer resources
                transformer_resources = {}
                if hasattr(transformer, "resources") and transformer.resources:
                    transformer_resources = {
                        "cores": transformer.resources.cores,
                        "memory": transformer.resources.memory,
                        "gpus": transformer.resources.gpus
                    }
                
                return {
                    "script_file": transformer.script_file if hasattr(transformer, "script_file") else script_file,
                    "resources": transformer_resources,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create transformer: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_inference_endpoints(
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get information about available inference endpoints.
            
            Returns:
                Information about inference endpoints
            """
            if ctx:
                await ctx.info("Getting available inference endpoints")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                
                # Get inference endpoints
                endpoints = ms.get_inference_endpoints()
                
                result = []
                for endpoint in endpoints:
                    result.append({
                        "name": endpoint.name if hasattr(endpoint, "name") else None,
                        "url": endpoint.url if hasattr(endpoint, "url") else None,
                        "protocol": endpoint.protocol if hasattr(endpoint, "protocol") else None,
                        "description": endpoint.description if hasattr(endpoint, "description") else None
                    })
                
                return {
                    "endpoints": result,
                    "count": len(result),
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get inference endpoints: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_deployment_url(
            name: str,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the URL for a deployment in the Hopsworks UI.
            
            Args:
                name: Name of the deployment
                
            Returns:
                URL information
            """
            if ctx:
                await ctx.info(f"Getting URL for deployment: {name}")
                
            try:
                project = hopsworks.get_current_project()
                ms = project.get_model_serving()
                deployment = ms.get_deployment(name=name)
                
                if not deployment:
                    return {
                        "status": "error",
                        "message": f"Deployment '{name}' not found"
                    }
                
                # Get the URL
                url = deployment.get_url()
                
                return {
                    "name": name,
                    "url": url,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get deployment URL: {str(e)}"
                }