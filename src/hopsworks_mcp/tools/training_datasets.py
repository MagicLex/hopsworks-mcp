"""Training datasets capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union, Literal
import json
import hopsworks
from fastmcp import Context


class TrainingDatasetTools:
    """Tools for working with Hopsworks training datasets."""

    def __init__(self, mcp):
        """Initialize training dataset tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_training_dataset(
            name: str,
            query_expression: str,
            description: str = "",
            data_format: Literal["tfrecords", "csv", "tsv", "parquet", "avro", "orc"] = "parquet",
            version: Optional[int] = None,
            coalesce: bool = False,
            splits: Optional[Dict[str, float]] = None,
            seed: Optional[int] = None,
            label: Optional[List[str]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a training dataset from a feature store query.
            
            NOTE: While this API still works, it's recommended to use feature views for new projects.
            
            Args:
                name: Name of the training dataset to create
                query_expression: Python-like expression to construct the query (e.g., "fg.select_all()")
                description: Description of the training dataset
                data_format: Format to use for the training dataset (parquet, csv, tfrecords, etc.)
                version: Version of the training dataset
                coalesce: Whether to coalesce data into a single file per split
                splits: Dictionary of split names and their proportions (e.g., {"train": 0.7, "test": 0.3})
                seed: Random seed for reproducible splits
                label: List of feature names that constitute the label/target
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Training dataset information
            """
            if ctx:
                await ctx.info(f"Creating training dataset: {name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Parse and execute the query expression to get a query object
                # This is highly simplified - a real implementation would need
                # a more robust approach to evaluate expressions safely
                
                # Define a local namespace with access to feature store and feature groups
                local_ns = {"fs": fs}
                
                # Get all feature groups to make them available in the namespace
                feature_groups_query = "SELECT name, version FROM feature_store_metadata.feature_group"
                fg_df = fs.sql(feature_groups_query, dataframe_type="pandas")
                
                await ctx.info("Evaluating query expression...")
                
                for _, row in fg_df.iterrows():
                    name_fg = row['name']
                    version = row['version']
                    fg_var_name = name_fg.replace("-", "_").replace(" ", "_").lower()
                    try:
                        # Try regular feature group
                        local_ns[fg_var_name] = fs.get_feature_group(name=name_fg, version=version)
                    except:
                        try:
                            # Try external feature group
                            local_ns[fg_var_name] = fs.get_external_feature_group(name=name_fg, version=version)
                        except:
                            # Skip if not found
                            pass
                
                # Execute the query expression to get a query object
                # Only evaluate if it looks safe (simplified check)
                if "import" in query_expression or "exec" in query_expression or "__" in query_expression:
                    return {
                        "status": "error",
                        "message": "Invalid query expression: contains potentially unsafe operations"
                    }
                
                query = eval(query_expression, {"__builtins__": {}}, local_ns)
                
                await ctx.info("Creating training dataset metadata...")
                
                # Create training dataset
                training_dataset = fs.create_training_dataset(
                    name=name,
                    version=version,
                    description=description,
                    data_format=data_format,
                    coalesce=coalesce,
                    splits=splits,
                    seed=seed,
                    label=label
                )
                
                await ctx.info("Saving training dataset...")
                
                # Save the training dataset with the query
                job = training_dataset.save(query)
                
                # If job is returned, wait for it to complete
                if job:
                    await ctx.info(f"Job started with ID: {job.id}. Waiting for completion...")
                    job.wait_for_completion()
                    status = job.get_status()
                    state = status['state']
                    
                    if state != 'SUCCEEDED':
                        return {
                            "status": "error",
                            "message": f"Job failed with state: {state}",
                            "job_id": job.id
                        }
                
                # Return metadata about the created training dataset
                feature_groups = []
                if hasattr(query, 'featuregroups'):
                    for fg in query.featuregroups:
                        feature_groups.append({
                            "name": fg.name,
                            "version": fg.version
                        })
                
                return {
                    "name": training_dataset.name,
                    "version": training_dataset.version,
                    "description": training_dataset.description,
                    "data_format": training_dataset.data_format,
                    "feature_groups": feature_groups,
                    "coalesce": coalesce,
                    "splits": splits,
                    "label": label,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create training dataset: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_training_dataset(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get information about a training dataset.
            
            Args:
                name: Name of the training dataset
                version: Version of the training dataset
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Training dataset information
            """
            if ctx:
                await ctx.info(f"Getting training dataset: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get training dataset
                training_dataset = fs.get_training_dataset(name=name, version=version)
                
                # Get training dataset schema
                schema = []
                if hasattr(training_dataset, 'schema'):
                    for feature in training_dataset.schema:
                        schema.append({
                            "name": feature.name,
                            "type": feature.type,
                            "description": feature.description if hasattr(feature, 'description') else None
                        })
                
                # Get training dataset statistics if available
                statistics = None
                try:
                    stats = training_dataset.statistics
                    if stats:
                        statistics = {
                            "computed_at": str(stats.computation_time) if hasattr(stats, 'computation_time') else None,
                            "features_count": len(stats.feature_descriptive_statistics) if hasattr(stats, 'feature_descriptive_statistics') else 0,
                        }
                except:
                    pass
                
                return {
                    "name": training_dataset.name,
                    "version": training_dataset.version,
                    "description": training_dataset.description if hasattr(training_dataset, 'description') else None,
                    "data_format": training_dataset.data_format if hasattr(training_dataset, 'data_format') else None,
                    "coalesce": training_dataset.coalesce if hasattr(training_dataset, 'coalesce') else None,
                    "splits": training_dataset.splits if hasattr(training_dataset, 'splits') else None,
                    "label": training_dataset.label if hasattr(training_dataset, 'label') else None,
                    "schema": schema,
                    "statistics": statistics,
                    "location": training_dataset.location if hasattr(training_dataset, 'location') else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get training dataset: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_training_dataset(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a training dataset.
            
            Args:
                name: Name of the training dataset to delete
                version: Version of the training dataset
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting training dataset: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get training dataset
                training_dataset = fs.get_training_dataset(name=name, version=version)
                
                # Delete training dataset
                training_dataset.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete training dataset: {str(e)}"
                }
        
        @self.mcp.tool()
        async def read_training_dataset(
            name: str,
            version: int = 1,
            split: Optional[str] = None,
            limit: int = 100,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Read data from a training dataset.
            
            Args:
                name: Name of the training dataset
                version: Version of the training dataset
                split: Name of the split to read (required if dataset has splits)
                limit: Maximum number of rows to return
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Data from the training dataset
            """
            if ctx:
                await ctx.info(f"Reading training dataset: {name} (v{version}){' split: ' + split if split else ''}")
            
            try:
                import pandas as pd
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get training dataset
                training_dataset = fs.get_training_dataset(name=name, version=version)
                
                # Read the data
                df = training_dataset.read(split=split)
                
                # Convert to pandas if it's a Spark dataframe
                if hasattr(df, 'toPandas'):
                    pdf = df.limit(limit).toPandas()
                else:
                    pdf = df.head(limit)
                
                # Convert to JSON for return
                data = json.loads(pdf.to_json(orient='records'))
                
                return {
                    "name": training_dataset.name,
                    "version": training_dataset.version,
                    "split": split,
                    "data": data,
                    "rows_count": len(pdf),
                    "truncated": len(df) > limit if hasattr(df, '__len__') else True,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to read training dataset: {str(e)}"
                }
        
        @self.mcp.tool()
        async def compute_training_dataset_statistics(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Compute statistics for a training dataset.
            
            Args:
                name: Name of the training dataset
                version: Version of the training dataset
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Statistics status information
            """
            if ctx:
                await ctx.info(f"Computing statistics for training dataset: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get training dataset
                training_dataset = fs.get_training_dataset(name=name, version=version)
                
                # Compute statistics
                stats = training_dataset.compute_statistics()
                
                # Format statistics information
                features_stats = []
                if hasattr(stats, 'feature_descriptive_statistics'):
                    for feature_stat in stats.feature_descriptive_statistics:
                        feat_stat = {
                            "name": feature_stat.feature_name,
                            "count": feature_stat.count if hasattr(feature_stat, 'count') else None,
                            "mean": feature_stat.mean if hasattr(feature_stat, 'mean') else None,
                            "min": feature_stat.min if hasattr(feature_stat, 'min') else None,
                            "max": feature_stat.max if hasattr(feature_stat, 'max') else None,
                            "std": feature_stat.stddev if hasattr(feature_stat, 'stddev') else None,
                            "completeness": feature_stat.completeness if hasattr(feature_stat, 'completeness') else None
                        }
                        features_stats.append(feat_stat)
                
                return {
                    "name": training_dataset.name,
                    "version": training_dataset.version,
                    "computed_at": str(stats.computation_time) if hasattr(stats, 'computation_time') else None,
                    "features_count": len(features_stats),
                    "features_statistics": features_stats,
                    "status": "computed"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to compute statistics: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_serving_vector(
            name: str,
            entry: Dict[str, Any],
            version: int = 1,
            external: Optional[bool] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a serving vector from the online feature store for a training dataset.
            
            Args:
                name: Name of the training dataset
                entry: Dictionary of primary key values (e.g., {"customer_id": 123})
                version: Version of the training dataset
                external: Whether to use external connection (default depends on connection)
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Feature vector for serving
            """
            if ctx:
                await ctx.info(f"Getting serving vector for training dataset: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get training dataset
                training_dataset = fs.get_training_dataset(name=name, version=version)
                
                # Initialize prepared statement for optimization (if not already initialized)
                training_dataset.init_prepared_statement(external=external)
                
                # Get serving vector
                serving_vector = training_dataset.get_serving_vector(entry=entry, external=external)
                
                # If serving vector is a Pandas DataFrame, convert to records
                if hasattr(serving_vector, 'to_dict'):
                    vector_data = serving_vector.to_dict(orient='records')
                elif hasattr(serving_vector, 'tolist'):
                    # If it's a list-like object (numpy array)
                    vector_data = serving_vector.tolist()
                else:
                    # If it's already a list
                    vector_data = serving_vector
                
                # Get the schema to have feature names
                features = []
                if hasattr(training_dataset, 'schema') and training_dataset.schema:
                    features = [f.name for f in training_dataset.schema]
                
                result = {
                    "name": training_dataset.name,
                    "version": training_dataset.version,
                    "entry": entry,
                    "serving_vector": vector_data,
                    "features": features,
                    "status": "success"
                }
                
                # If the result is too large, truncate it
                if len(str(result)) > 50000:
                    result["status"] = "success_truncated"
                    result["message"] = "Result was truncated due to size"
                    if isinstance(vector_data, list) and len(vector_data) > 100:
                        result["serving_vector"] = vector_data[:100]
                
                return result
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get serving vector: {str(e)}"
                }