"""Feature views capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union
import json
import hopsworks
from fastmcp import Context


class FeatureViewTools:
    """Tools for working with Hopsworks Feature Views."""

    def __init__(self, mcp):
        """Initialize feature view tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_feature_view(
            name: str,
            query: str,
            version: Optional[int] = None,
            description: str = "",
            labels: Optional[List[str]] = None,
            transformation_functions: Optional[List[Any]] = None,
            inference_helper_columns: Optional[List[str]] = None,
            training_helper_columns: Optional[List[str]] = None,
            logging_enabled: bool = False,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a feature view metadata object and save it to Hopsworks.
            
            A feature view is a logical view over feature groups that can be used
            for model training and inference.
            
            Args:
                name: Name of the feature view to create
                query: Feature store query object (serialized as string)
                version: Version of the feature view (defaults to incremented from last version)
                description: Description of the feature view
                labels: List of feature names that are prediction targets/labels
                transformation_functions: List of transformation functions to attach to the feature view
                    These model-dependent transformation functions are applied when using the feature view
                    for training and inference.
                inference_helper_columns: List of feature names that are not used for training but are needed
                    during inference for computing on-demand features.
                training_helper_columns: List of feature names that provide additional information during
                    training but are not part of the model features.
                logging_enabled: Whether to enable feature and prediction logging (default: False)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature view information
            """
            if ctx:
                await ctx.info(f"Creating feature view: {name} (v{version or 'auto'}) in project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Convert empty lists to None to avoid API issues
                if labels is not None and len(labels) == 0:
                    labels = None
                    
                if transformation_functions is not None and len(transformation_functions) == 0:
                    transformation_functions = None
                
                # Build parameters for feature view creation
                fv_params = {
                    "name": name,
                    "version": version,
                    "description": description,
                    "labels": labels,
                    "query": eval(query)  # Dangerous in production, would need proper deserialization
                }
                
                # Add transformation functions if provided
                if transformation_functions is not None:
                    fv_params["transformation_functions"] = transformation_functions
                
                # Add helper columns if provided
                if inference_helper_columns is not None:
                    fv_params["inference_helper_columns"] = inference_helper_columns
                
                if training_helper_columns is not None:
                    fv_params["training_helper_columns"] = training_helper_columns
                
                # Add logging flag if enabled
                if logging_enabled:
                    fv_params["logging_enabled"] = logging_enabled
                
                # Create the feature view
                feature_view = fs.create_feature_view(**fv_params)
                
                return {
                    "name": feature_view.name,
                    "version": feature_view.version,
                    "description": feature_view.description,
                    "labels": feature_view.labels or [],
                    "created": str(feature_view.creation_date) if hasattr(feature_view, 'creation_date') else None,
                    "feature_store_name": feature_view.feature_store_name,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create feature view: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_feature_view(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a feature view entity from the feature store.
            
            Args:
                name: Name of the feature view to get
                version: Version of the feature view to retrieve (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature view details
            """
            if ctx:
                await ctx.info(f"Getting feature view: {name} (v{version}) from project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                return {
                    "name": fv.name,
                    "version": fv.version,
                    "description": fv.description,
                    "labels": fv.labels or [],
                    "feature_store_name": fv.feature_store_name,
                    "schema": list(fv.schema) if hasattr(fv, 'schema') else [],
                    "query": str(fv.query) if hasattr(fv, 'query') else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature view: {str(e)}"
                }
                
        @self.mcp.tool()
        async def list_feature_views(
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List feature views in a Hopsworks project's feature store.
            
            Args:
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                list: List of feature views in the feature store
            """
            if ctx:
                await ctx.info(f"Listing feature views for project: {project_name or 'default'}")
                
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature views - since there's no direct method to get all feature views,
                # we need to implement a workaround using SQL query on metadata
                # This is a placeholder - in a real implementation, better approach is needed
                query = "SELECT name FROM feature_store_metadata.feature_views GROUP BY name"
                df = fs.sql(query, dataframe_type="pandas")
                fv_names = df['name'].tolist()
                
                result = []
                for fv_name in fv_names:
                    try:
                        # Get all versions for each feature view name
                        fvs = fs.get_feature_views(fv_name)
                        for fv in fvs:
                            result.append({
                                "name": fv.name,
                                "version": fv.version,
                                "description": fv.description,
                                "labels": fv.labels or [],
                                "feature_store_name": fv.feature_store_name
                            })
                    except:
                        # Skip any feature views that can't be retrieved
                        pass
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list feature views: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def update_feature_view_description(
            name: str,
            description: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the description of a feature view.
            
            Args:
                name: Name of the feature view to update
                description: New description for the feature view
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Updated feature view information
            """
            if ctx:
                await ctx.info(f"Updating description for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Update description
                fv.description = description
                fv.update()
                
                return {
                    "name": fv.name,
                    "version": fv.version,
                    "description": fv.description,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update feature view description: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_feature_view(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a feature view and all associated metadata and training data.
            
            Args:
                name: Name of the feature view to delete
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Delete feature view
                fv.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete feature view: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_batch_data(
            name: str,
            version: int = 1,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            limit: int = 100,
            transform: bool = True,
            primary_key: bool = False,
            event_time: bool = False,
            inference_helpers: bool = False,
            training_helpers: bool = False,
            read_options: Optional[Dict[str, Any]] = None,
            transformation_context: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a batch of data from a feature view.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                start_time: Start event time for the batch query (format: YYYY-MM-DD HH:MM:SS)
                end_time: End event time for the batch query (format: YYYY-MM-DD HH:MM:SS)
                limit: Maximum number of rows to return
                transform: Whether to apply model-dependent transformations (default: True)
                primary_key: Whether to include primary key columns in the result (default: False)
                event_time: Whether to include event time column in the result (default: False)
                inference_helpers: Whether to include inference helper columns in the result (default: False)
                training_helpers: Whether to include training helper columns in the result (default: False)
                read_options: Additional options for reading data (e.g., {"use_hive": True})
                transformation_context: Context variables to pass to transformation functions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Batch data from the feature view
            """
            if ctx:
                await ctx.info(f"Getting batch data from feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for get_batch_data
                params = {
                    "start_time": start_time,
                    "end_time": end_time,
                    "transform": transform
                }
                
                # Add optional parameters if provided
                if primary_key:
                    params["primary_key"] = primary_key
                
                if event_time:
                    params["event_time"] = event_time
                
                if inference_helpers:
                    params["inference_helpers"] = inference_helpers
                
                if training_helpers:
                    params["training_helpers"] = training_helpers
                
                if read_options is not None:
                    params["read_options"] = read_options
                
                if transformation_context is not None:
                    params["transformation_context"] = transformation_context
                
                # Get batch data
                df = fv.get_batch_data(**params)
                
                # Convert result to JSON format
                rows = json.loads(df.head(limit).to_json(orient="records"))
                
                return {
                    "name": fv.name,
                    "version": fv.version,
                    "rows": rows,
                    "count": len(rows),
                    "truncated": len(rows) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get batch data: {str(e)}"
                }
                
        @self.mcp.tool()
        async def create_training_data(
            name: str,
            version: int = 1,
            description: str = "",
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            data_format: str = "parquet",
            primary_key: bool = False,
            event_time: bool = False,
            inference_helpers: bool = False,
            training_helpers: bool = False,
            write_options: Optional[Dict[str, Any]] = None,
            transformation_context: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create training data from a feature view.
            
            This materializes a training dataset from the feature view's data that
            can be used to train machine learning models.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                description: Description of the training dataset
                start_time: Start event time for data selection (format: YYYY-MM-DD HH:MM:SS)
                end_time: End event time for data selection (format: YYYY-MM-DD HH:MM:SS)
                data_format: Format to save the data (parquet, csv, tfrecord, etc.)
                primary_key: Whether to include primary key columns in the result (default: False)
                event_time: Whether to include event time column in the result (default: False)
                inference_helpers: Whether to include inference helper columns in the result (default: False)
                training_helpers: Whether to include training helper columns in the result (default: False)
                write_options: Additional options for writing data (e.g., {"wait_for_job": False})
                transformation_context: Context variables to pass to transformation functions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the created training data
            """
            if ctx:
                await ctx.info(f"Creating training data from feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for create_training_data
                params = {
                    "description": description,
                    "start_time": start_time,
                    "end_time": end_time,
                    "data_format": data_format
                }
                
                # Add optional parameters if provided
                if primary_key:
                    params["primary_key"] = primary_key
                
                if event_time:
                    params["with_event_time"] = event_time
                
                if inference_helpers:
                    params["inference_helper_columns"] = inference_helpers
                
                if training_helpers:
                    params["training_helper_columns"] = training_helpers
                
                if write_options is not None:
                    params["write_options"] = write_options
                
                if transformation_context is not None:
                    params["transformation_context"] = transformation_context
                
                # Create training data
                training_dataset_version, job = fv.create_training_data(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "job_id": job.id if job else None,
                    "job_name": job.name if job else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create training data: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_training_data(
            name: str,
            version: int = 1,
            training_dataset_version: int = 1,
            primary_key: bool = False,
            event_time: bool = False,
            inference_helpers: bool = False,
            training_helpers: bool = False,
            read_options: Optional[Dict[str, Any]] = None,
            transformation_context: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get training data from a feature view.
            
            This retrieves previously materialized training data from a feature view.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                training_dataset_version: Version of the training dataset to retrieve
                primary_key: Whether to include primary key columns in the result (default: False)
                event_time: Whether to include event time column in the result (default: False)
                inference_helpers: Whether to include inference helper columns in the result (default: False)
                training_helpers: Whether to include training helper columns in the result (default: False)
                read_options: Additional options for reading data (e.g., {"use_hive": True})
                transformation_context: Context variables to pass to transformation functions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the retrieved training data
            """
            if ctx:
                await ctx.info(f"Getting training data from feature view: {name} (v{version}), training dataset v{training_dataset_version}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for get_training_data
                params = {
                    "training_dataset_version": training_dataset_version
                }
                
                # Add optional parameters if provided
                if primary_key:
                    params["primary_key"] = primary_key
                
                if event_time:
                    params["event_time"] = event_time
                
                if inference_helpers:
                    params["inference_helper_columns"] = inference_helpers
                
                if training_helpers:
                    params["training_helper_columns"] = training_helpers
                
                if read_options is not None:
                    params["read_options"] = read_options
                
                if transformation_context is not None:
                    params["transformation_context"] = transformation_context
                
                # Get training data
                features_df, labels_df = fv.get_training_data(**params)
                
                # Result will include information about the training data but not the actual data
                # as it could be very large
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "features_count": len(features_df.columns) if features_df is not None else 0,
                    "labels_count": len(labels_df.columns) if labels_df is not None else 0,
                    "rows_count": len(features_df) if features_df is not None else 0,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get training data: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_feature_vector(
            name: str,
            version: int = 1,
            entry: Dict[str, Any] = None,
            transform: bool = True,
            on_demand_features: bool = True,
            request_parameter: Optional[Dict[str, Any]] = None,
            return_type: str = "dict",
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a feature vector from the online feature store.
            
            This retrieves a feature vector for online serving from the feature view,
            with options to compute on-demand features and apply transformations.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                entry: Dictionary of primary key values to retrieve the feature vector for
                transform: Whether to apply model-dependent transformations (default: True)
                on_demand_features: Whether to compute on-demand features (default: True)
                request_parameter: Parameters for on-demand feature computation (e.g., current dates/times)
                return_type: Return type format - "dict" or "pandas" (default: "dict")
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature vector information
            """
            if ctx:
                await ctx.info(f"Getting feature vector from feature view: {name} (v{version})")
                if request_parameter:
                    await ctx.info(f"With on-demand parameters: {request_parameter}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Build parameters for get_feature_vector
                params = {
                    "entry": entry,
                    "transform": transform,
                    "on_demand_features": on_demand_features,
                    "return_type": return_type
                }
                
                # Add request_parameter if provided
                if request_parameter is not None:
                    params["request_parameter"] = request_parameter
                
                # Get feature vector
                vector = fv.get_feature_vector(**params)
                
                # Convert pandas to dict if needed
                if return_type == "pandas" and hasattr(vector, 'to_dict'):
                    vector_data = vector.to_dict('records')[0] if len(vector) > 0 else {}
                else:
                    vector_data = vector
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "vector": vector_data,
                    "on_demand_features_computed": on_demand_features,
                    "transformations_applied": transform,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature vector: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_feature_vectors(
            name: str,
            version: int = 1,
            entries: List[Dict[str, Any]] = None,
            transform: bool = True,
            on_demand_features: bool = True,
            request_parameter: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            return_type: str = "dict",
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get multiple feature vectors from the online feature store.
            
            This retrieves multiple feature vectors for online serving from the feature view,
            with options to compute on-demand features and apply transformations.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                entries: List of dictionaries containing primary key values to retrieve feature vectors for
                transform: Whether to apply model-dependent transformations (default: True)
                on_demand_features: Whether to compute on-demand features (default: True)
                request_parameter: Parameters for on-demand feature computation, either:
                    - A single dictionary (applied to all entries)
                    - A list of dictionaries (one per entry)
                return_type: Return type format - "dict" or "pandas" (default: "dict")
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature vectors information
            """
            if ctx:
                await ctx.info(f"Getting feature vectors from feature view: {name} (v{version})")
                if request_parameter:
                    if isinstance(request_parameter, list):
                        await ctx.info(f"With {len(request_parameter)} sets of on-demand parameters")
                    else:
                        await ctx.info(f"With common on-demand parameters")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Build parameters for get_feature_vectors
                params = {
                    "entries": entries,
                    "transform": transform,
                    "on_demand_features": on_demand_features,
                    "return_type": return_type
                }
                
                # Add request_parameter if provided
                if request_parameter is not None:
                    params["request_parameter"] = request_parameter
                
                # Get feature vectors
                vectors = fv.get_feature_vectors(**params)
                
                # Convert pandas to dict if needed
                if return_type == "pandas" and hasattr(vectors, 'to_dict'):
                    vectors_data = vectors.to_dict('records') if len(vectors) > 0 else []
                else:
                    vectors_data = vectors
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "vectors": vectors_data,
                    "count": len(vectors_data) if isinstance(vectors_data, list) else 0,
                    "on_demand_features_computed": on_demand_features,
                    "transformations_applied": transform,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature vectors: {str(e)}"
                }
                
        @self.mcp.tool()
        async def compute_on_demand_features(
            name: str,
            version: int = 1,
            feature_vector: Dict[str, Any] = None,
            request_parameter: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Compute on-demand features for a single feature vector.
            
            This computes on-demand features for an existing feature vector without
            applying model-dependent transformations.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                feature_vector: Feature vector to compute on-demand features for
                request_parameter: Parameters for on-demand feature computation
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature vector with on-demand features
            """
            if ctx:
                await ctx.info(f"Computing on-demand features for feature view: {name} (v{version})")
                if request_parameter:
                    await ctx.info(f"With on-demand parameters: {request_parameter}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Build parameters for compute_on_demand_features
                params = {
                    "feature_vector": feature_vector
                }
                
                # Add request_parameter if provided
                if request_parameter is not None:
                    params["request_parameter"] = request_parameter
                
                # Compute on-demand features
                result = fv.compute_on_demand_features(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "feature_vector": result,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to compute on-demand features: {str(e)}"
                }
                
        @self.mcp.tool()
        async def compute_on_demand_features_batch(
            name: str,
            version: int = 1,
            feature_vectors: List[Dict[str, Any]] = None,
            request_parameter: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Compute on-demand features for multiple feature vectors.
            
            This computes on-demand features for existing feature vectors without
            applying model-dependent transformations.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                feature_vectors: List of feature vectors to compute on-demand features for
                request_parameter: Parameters for on-demand feature computation, either:
                    - A single dictionary (applied to all feature vectors)
                    - A list of dictionaries (one per feature vector)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature vectors with on-demand features
            """
            if ctx:
                await ctx.info(f"Computing on-demand features for feature view: {name} (v{version})")
                if request_parameter:
                    if isinstance(request_parameter, list):
                        await ctx.info(f"With {len(request_parameter)} sets of on-demand parameters")
                    else:
                        await ctx.info(f"With common on-demand parameters")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Build parameters for compute_on_demand_features
                params = {
                    "feature_vectors": feature_vectors
                }
                
                # Add request_parameter if provided
                if request_parameter is not None:
                    params["request_parameter"] = request_parameter
                
                # Compute on-demand features
                results = fv.compute_on_demand_features(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "feature_vectors": results,
                    "count": len(results) if isinstance(results, list) else 0,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to compute on-demand features in batch: {str(e)}"
                }
                
        @self.mcp.tool()
        async def transform(
            name: str,
            version: int = 1,
            feature_vector: Dict[str, Any] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Apply model-dependent transformations to a feature vector.
            
            This applies model-dependent transformations to a feature vector
            that may already have on-demand features computed.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                feature_vector: Feature vector to transform
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Transformed feature vector
            """
            if ctx:
                await ctx.info(f"Applying transformations for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Apply transformations
                result = fv.transform(feature_vector)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "feature_vector": result,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to transform feature vector: {str(e)}"
                }
                
        @self.mcp.tool()
        async def transform_batch(
            name: str,
            version: int = 1,
            feature_vectors: List[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Apply model-dependent transformations to multiple feature vectors.
            
            This applies model-dependent transformations to feature vectors
            that may already have on-demand features computed.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                feature_vectors: List of feature vectors to transform
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Transformed feature vectors
            """
            if ctx:
                await ctx.info(f"Applying transformations for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Apply transformations
                results = fv.transform(feature_vectors)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "feature_vectors": results,
                    "count": len(results) if isinstance(results, list) else 0,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to transform feature vectors in batch: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_inference_helper(
            name: str,
            version: int = 1,
            entry: Dict[str, Any] = None,
            return_type: str = "dict",
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get inference helper columns for a specified key from a feature view.
            
            This retrieves only the inference helper columns from a feature view for a given entry.
            These helper columns can be used to compute on-demand features during inference.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                entry: Dictionary of primary key values to retrieve inference helpers for
                return_type: Return type format - "dict" or "pandas" (default: "dict")
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Inference helper columns for the specified key
            """
            if ctx:
                await ctx.info(f"Getting inference helper columns from feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize serving
                fv.init_serving()
                
                # Get inference helpers
                result = fv.get_inference_helper(entry=entry, return_type=return_type)
                
                # Convert pandas to dict if needed
                if return_type == "pandas" and hasattr(result, 'to_dict'):
                    helper_data = result.to_dict('records')[0] if len(result) > 0 else {}
                else:
                    helper_data = result
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "helpers": helper_data,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get inference helper columns: {str(e)}"
                }
                
        @self.mcp.tool()
        async def init_serving(
            name: str,
            version: int = 1,
            training_dataset_version: Optional[int] = None,
            init_rest_client: bool = False,
            init_sql_client: bool = True,
            config_rest_client: Optional[Dict[str, Any]] = None,
            default_client: str = "sql",
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Initialize serving for a feature view.
            
            This initializes the serving client and loads any transformation statistics
            needed for model-dependent transformations.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                training_dataset_version: Version of the training dataset to use for transformation statistics
                init_rest_client: Whether to initialize the REST client (default: False)
                init_sql_client: Whether to initialize the SQL client (default: True)
                config_rest_client: Configuration options for the REST client
                default_client: Default client to use ("sql" or "rest")
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the initialized serving
            """
            if ctx:
                await ctx.info(f"Initializing serving for feature view: {name} (v{version})")
                if training_dataset_version:
                    await ctx.info(f"Using training dataset version: {training_dataset_version}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for init_serving
                params = {}
                
                if training_dataset_version is not None:
                    params["training_dataset_version"] = training_dataset_version
                
                # Setup client initialization parameters
                if init_rest_client:
                    params["init_rest_client"] = init_rest_client
                
                if init_sql_client:
                    params["init_sql_client"] = init_sql_client
                
                if config_rest_client is not None:
                    params["config_rest_client"] = config_rest_client
                
                if default_client:
                    params["default_client"] = default_client
                
                # Initialize serving
                fv.init_serving(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "default_client": default_client,
                    "status": "initialized"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to initialize serving: {str(e)}"
                }
                
        @self.mcp.tool()
        async def init_batch_scoring(
            name: str,
            training_dataset_version: int,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Initialize batch scoring for a feature view.
            
            This loads transformation statistics needed for model-dependent transformations
            during batch inference.
            
            Args:
                name: Name of the feature view
                training_dataset_version: Version of the training dataset to use for transformation statistics
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the initialized batch scoring
            """
            if ctx:
                await ctx.info(f"Initializing batch scoring for feature view: {name} (v{version})")
                await ctx.info(f"Using training dataset version: {training_dataset_version}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Initialize batch scoring
                fv.init_batch_scoring(training_dataset_version=training_dataset_version)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "status": "initialized"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to initialize batch scoring: {str(e)}"
                }
                
        @self.mcp.tool()
        async def enable_logging(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Enable feature and prediction logging for a feature view.
            
            This creates and configures the required feature groups for storing logs.
            Logs are written to the offline feature store periodically by scheduled jobs.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the logging configuration
            """
            if ctx:
                await ctx.info(f"Enabling logging for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Enable logging
                fv.enable_logging()
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "logging_enabled": True,
                    "status": "enabled"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to enable logging: {str(e)}"
                }
                
        @self.mcp.tool()
        async def log_features(
            name: str,
            version: int = 1,
            features: Optional[Dict[str, Any]] = None,
            transformed_features: Optional[Dict[str, Any]] = None,
            predictions: Optional[Dict[str, Any]] = None,
            training_dataset_version: Optional[int] = None,
            model_name: Optional[str] = None,
            model_version: Optional[int] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Log features and predictions for monitoring and debugging.
            
            This logs untransformed features, transformed features, and/or predictions
            to the feature store for later analysis and monitoring.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                features: Dictionary of untransformed features to log (null if not provided)
                transformed_features: Dictionary of transformed features to log (null if not provided)
                predictions: Dictionary of predictions to log (null if not provided)
                training_dataset_version: Version of the training dataset used with the model
                model_name: Name of the model used for predictions
                model_version: Version of the model used for predictions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the logged data
            """
            if ctx:
                await ctx.info(f"Logging features for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Convert inputs to pandas DataFrames if they're dictionaries
                import pandas as pd
                
                log_params = {}
                
                # Add untransformed features if provided
                if features is not None:
                    if isinstance(features, dict):
                        features_df = pd.DataFrame([features])
                        log_params["features"] = features_df
                    else:
                        log_params["features"] = features
                
                # Add transformed features if provided
                if transformed_features is not None:
                    if isinstance(transformed_features, dict):
                        transformed_features_df = pd.DataFrame([transformed_features])
                        log_params["transformed_features"] = transformed_features_df
                    else:
                        log_params["transformed_features"] = transformed_features
                
                # Add predictions if provided
                if predictions is not None:
                    if isinstance(predictions, dict):
                        predictions_df = pd.DataFrame([predictions])
                        log_params["predictions"] = predictions_df
                    else:
                        log_params["predictions"] = predictions
                
                # Add training dataset version if provided
                if training_dataset_version is not None:
                    log_params["training_dataset_version"] = training_dataset_version
                
                # Add model info if provided
                if model_name is not None and model_version is not None:
                    # Create a Model object if model info provided
                    from hopsworks.model_registry import Model
                    model = Model(id=None, name=model_name, version=model_version)
                    log_params["model"] = model
                
                # Log the features and predictions
                fv.log(**log_params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "features_logged": features is not None,
                    "transformed_features_logged": transformed_features is not None,
                    "predictions_logged": predictions is not None,
                    "status": "logged"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to log features: {str(e)}"
                }
                
        @self.mcp.tool()
        async def materialize_log(
            name: str,
            version: int = 1,
            transformed: bool = None,
            wait: bool = True,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Materialize feature and prediction logs to the offline feature store.
            
            By default, logs are written to the offline store every hour by scheduled jobs.
            This method triggers an immediate materialization.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                transformed: Whether to materialize transformed (True) or untransformed (False) logs,
                             or both if not specified (None)
                wait: Whether to wait for the materialization to complete (default: True)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the materialization process
            """
            if ctx:
                await ctx.info(f"Materializing logs for feature view: {name} (v{version})")
                if transformed is not None:
                    await ctx.info(f"Materializing {'transformed' if transformed else 'untransformed'} logs")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for materialize_log
                params = {
                    "wait": wait
                }
                
                if transformed is not None:
                    params["transformed"] = transformed
                
                # Materialize logs
                result = fv.materialize_log(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "materialization_result": str(result) if result else None,
                    "transformed": transformed,
                    "status": "materialized"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to materialize logs: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_log_timeline(
            name: str,
            version: int = 1,
            limit: int = 10,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the timeline of feature and prediction logs.
            
            This retrieves metadata about when logs were written to the feature store.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                limit: Maximum number of timeline entries to return (default: 10)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Log timeline information
            """
            if ctx:
                await ctx.info(f"Getting log timeline for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Get log timeline
                timeline = fv.get_log_timeline(limit=limit)
                
                # Format the timeline data
                timeline_entries = []
                for entry in timeline:
                    if hasattr(entry, 'to_dict'):
                        timeline_entries.append(entry.to_dict())
                    else:
                        timeline_entries.append(str(entry))
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "timeline": timeline_entries,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get log timeline: {str(e)}"
                }
                
        @self.mcp.tool()
        async def read_log(
            name: str,
            version: int = 1,
            start_time: Optional[str] = None,
            end_time: Optional[str] = None,
            training_dataset_version: Optional[int] = None,
            model_name: Optional[str] = None,
            model_version: Optional[int] = None,
            filter_expression: Optional[str] = None,
            transformed: bool = None,
            limit: int = 100,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Read logged features and predictions from the feature store.
            
            This retrieves previously logged data for analysis and monitoring.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                start_time: Start time for filtering logs (format: YYYY-MM-DD HH:MM:SS)
                end_time: End time for filtering logs (format: YYYY-MM-DD HH:MM:SS)
                training_dataset_version: Filter by training dataset version
                model_name: Filter by model name
                model_version: Filter by model version
                filter_expression: Custom filter expression
                transformed: Whether to read transformed (True) or untransformed (False) logs,
                             or both if not specified (None)
                limit: Maximum number of log entries to return
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Logged features and predictions
            """
            if ctx:
                await ctx.info(f"Reading logs for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for read_log
                params = {}
                
                if start_time:
                    params["start_time"] = start_time
                
                if end_time:
                    params["end_time"] = end_time
                
                if training_dataset_version is not None:
                    params["training_dataset_version"] = training_dataset_version
                
                if model_name is not None and model_version is not None:
                    from hopsworks.model_registry import Model
                    model = Model(id=None, name=model_name, version=model_version)
                    params["model"] = model
                
                if filter_expression:
                    # Note: This is a simplification and may need to be implemented differently
                    # based on how filter expressions are handled in the actual API
                    params["filter"] = eval(filter_expression)
                
                if transformed is not None:
                    params["transformed"] = transformed
                
                # Read logs
                log_df = fv.read_log(**params)
                
                # Convert to JSON format
                import json
                rows = json.loads(log_df.head(limit).to_json(orient="records"))
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "rows": rows,
                    "count": len(rows),
                    "truncated": len(rows) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to read logs: {str(e)}"
                }
                
        @self.mcp.tool()
        async def pause_logging(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Pause the scheduled materialization of logs to the offline feature store.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Pausing logging for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Pause logging
                fv.pause_logging()
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "status": "paused"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to pause logging: {str(e)}"
                }
                
        @self.mcp.tool()
        async def resume_logging(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Resume the scheduled materialization of logs to the offline feature store.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Resuming logging for feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Resume logging
                fv.resume_logging()
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "status": "resumed"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to resume logging: {str(e)}"
                }
                
        @self.mcp.tool()
        async def delete_log(
            name: str,
            version: int = 1,
            transformed: bool = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete all logged features and predictions.
            
            This deletes both the feature groups and the data stored in them.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                transformed: Whether to delete transformed (True) or untransformed (False) logs,
                             or both if not specified (None)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting logs for feature view: {name} (v{version})")
                if transformed is not None:
                    await ctx.info(f"Deleting {'transformed' if transformed else 'untransformed'} logs")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for delete_log
                params = {}
                
                if transformed is not None:
                    params["transformed"] = transformed
                
                # Delete logs
                fv.delete_log(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "transformed": transformed,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete logs: {str(e)}"
                }
                
        @self.mcp.tool()
        async def create_train_test_split(
            name: str,
            version: int = 1,
            test_size: float = 0.2,
            description: str = "",
            data_format: str = "parquet",
            primary_key: bool = False,
            event_time: bool = False,
            inference_helpers: bool = False,
            training_helpers: bool = False,
            write_options: Optional[Dict[str, Any]] = None,
            transformation_context: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a train-test split from a feature view.
            
            This materializes a training dataset split into train and test sets.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                test_size: Size of the test set as a fraction (0.0 to 1.0)
                description: Description of the training dataset
                data_format: Format to save the data (parquet, csv, tfrecord, etc.)
                primary_key: Whether to include primary key columns in the result (default: False)
                event_time: Whether to include event time column in the result (default: False)
                inference_helpers: Whether to include inference helper columns in the result (default: False)
                training_helpers: Whether to include training helper columns in the result (default: False)
                write_options: Additional options for writing data (e.g., {"wait_for_job": False})
                transformation_context: Context variables to pass to transformation functions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the created train-test split
            """
            if ctx:
                await ctx.info(f"Creating train-test split from feature view: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for create_train_test_split
                params = {
                    "test_size": test_size,
                    "description": description,
                    "data_format": data_format
                }
                
                # Add optional parameters if provided
                if primary_key:
                    params["primary_key"] = primary_key
                
                if event_time:
                    params["with_event_time"] = event_time
                
                if inference_helpers:
                    params["inference_helper_columns"] = inference_helpers
                
                if training_helpers:
                    params["training_helper_columns"] = training_helpers
                
                if write_options is not None:
                    params["write_options"] = write_options
                
                if transformation_context is not None:
                    params["transformation_context"] = transformation_context
                
                # Create train-test split
                training_dataset_version, job = fv.create_train_test_split(**params)
                
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "test_size": test_size,
                    "job_id": job.id if job else None,
                    "job_name": job.name if job else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create train-test split: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_train_test_split(
            name: str,
            version: int = 1,
            training_dataset_version: int = 1,
            primary_key: bool = False,
            event_time: bool = False,
            inference_helpers: bool = False,
            training_helpers: bool = False,
            read_options: Optional[Dict[str, Any]] = None,
            transformation_context: Optional[Dict[str, Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a train-test split from a feature view.
            
            This retrieves previously materialized train-test split data from a feature view.
            
            Args:
                name: Name of the feature view
                version: Version of the feature view (defaults to 1)
                training_dataset_version: Version of the training dataset to retrieve
                primary_key: Whether to include primary key columns in the result (default: False)
                event_time: Whether to include event time column in the result (default: False)
                inference_helpers: Whether to include inference helper columns in the result (default: False)
                training_helpers: Whether to include training helper columns in the result (default: False)
                read_options: Additional options for reading data (e.g., {"use_hive": True})
                transformation_context: Context variables to pass to transformation functions
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the retrieved train-test split
            """
            if ctx:
                await ctx.info(f"Getting train-test split from feature view: {name} (v{version}), training dataset v{training_dataset_version}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fv = fs.get_feature_view(name=name, version=version)
                
                # Build parameters for get_train_test_split
                params = {
                    "training_dataset_version": training_dataset_version
                }
                
                # Add optional parameters if provided
                if primary_key:
                    params["primary_key"] = primary_key
                
                if event_time:
                    params["event_time"] = event_time
                
                if inference_helpers:
                    params["inference_helper_columns"] = inference_helpers
                
                if training_helpers:
                    params["training_helper_columns"] = training_helpers
                
                if read_options is not None:
                    params["read_options"] = read_options
                
                if transformation_context is not None:
                    params["transformation_context"] = transformation_context
                
                # Get train-test split
                X_train, X_test, y_train, y_test = fv.get_train_test_split(**params)
                
                # Result will include information about the train-test split but not the actual data
                return {
                    "feature_view_name": name,
                    "feature_view_version": version,
                    "training_dataset_version": training_dataset_version,
                    "X_train_shape": (len(X_train), len(X_train.columns)) if X_train is not None else None,
                    "X_test_shape": (len(X_test), len(X_test.columns)) if X_test is not None else None,
                    "y_train_shape": (len(y_train), len(y_train.columns)) if y_train is not None and hasattr(y_train, 'columns') else None,
                    "y_test_shape": (len(y_test), len(y_test.columns)) if y_test is not None and hasattr(y_test, 'columns') else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get train-test split: {str(e)}"
                }