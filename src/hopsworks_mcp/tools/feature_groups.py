"""Feature group capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union
import json
import hopsworks
from fastmcp import Context


class FeatureGroupTools:
    """Tools for working with Hopsworks Feature Groups."""

    def __init__(self, mcp):
        """Initialize feature group tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_feature_group(
            name: str,
            description: str = "",
            version: Optional[int] = None,
            primary_key: Optional[List[str]] = None,
            partition_key: Optional[List[str]] = None,
            event_time: Optional[str] = None,
            online_enabled: bool = False,
            time_travel_format: str = "HUDI",
            statistics_config: Optional[Dict[str, Any]] = None,
            transformation_functions: Optional[List[Any]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a feature group metadata object.
            
            This method creates a feature group metadata object in the feature store.
            Note that this is a lazy operation - to save data to the feature group,
            you need to call insert later.
            
            Args:
                name: Name of the feature group to create
                description: Description of the feature group
                version: Version of the feature group (defaults to incremented from last version)
                primary_key: List of feature names to use as primary key
                partition_key: List of feature names to use as partition key
                event_time: Name of the feature containing event time 
                online_enabled: Whether the feature group should be available in online feature store
                time_travel_format: Format used for time travel (defaults to "HUDI")
                statistics_config: Configuration for feature statistics computation:
                    - enabled: Whether to compute statistics (default: True for descriptive statistics)
                    - correlations: Whether to compute correlations between features (default: False)
                    - histograms: Whether to compute histograms of feature values (default: False)
                    - exact_uniqueness: Whether to compute exact uniqueness statistics (default: False)
                    - columns: List of columns for which to compute statistics (default: all columns)
                transformation_functions: List of transformation functions to attach to the feature group
                    These functions will be used as on-demand transformations that can be dynamically
                    computed during online inference. Functions can be provided directly or with specified
                    input feature mappings.
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature group information
            """
            if ctx:
                await ctx.info(f"Creating feature group: {name} (v{version or 'auto'}) in project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Convert empty lists to None to avoid API issues
                if primary_key is not None and len(primary_key) == 0:
                    primary_key = None
                if partition_key is not None and len(partition_key) == 0:
                    partition_key = None
                
                # Build parameters for feature group creation
                fg_params = {
                    "name": name,
                    "description": description,
                    "version": version,
                    "primary_key": primary_key,
                    "partition_key": partition_key,
                    "event_time": event_time,
                    "online_enabled": online_enabled,
                    "time_travel_format": time_travel_format
                }
                
                # Add statistics config if provided
                if statistics_config is not None:
                    fg_params["statistics_config"] = statistics_config
                
                # Add transformation functions if provided
                if transformation_functions is not None:
                    fg_params["transformation_functions"] = transformation_functions
                
                # Create feature group
                feature_group = fs.create_feature_group(**fg_params)
                
                return {
                    "name": feature_group.name,
                    "version": feature_group.version,
                    "description": feature_group.description,
                    "primary_key": feature_group.primary_key or [],
                    "partition_key": feature_group.partition_key or [],
                    "event_time": feature_group.event_time,
                    "online_enabled": feature_group.online_enabled,
                    "time_travel_format": feature_group.time_travel_format,
                    "created": str(feature_group.created) if feature_group.created else None,
                    "creator": feature_group.creator,
                    "id": feature_group.id,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create feature group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_feature_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a feature group entity from the feature store.
            
            Getting a feature group means retrieving its metadata so you can 
            perform operations like reading data or joining with other feature groups.
            
            Args:
                name: Name of the feature group to get
                version: Version of the feature group to retrieve (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature group details
            """
            if ctx:
                await ctx.info(f"Getting feature group: {name} (v{version}) from project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Get features in a serializable format
                features = []
                for feature in fg.features:
                    features.append({
                        "name": feature.name,
                        "type": feature.type,
                        "description": feature.description,
                        "primary": feature.name in (fg.primary_key or []),
                        "partition": feature.name in (fg.partition_key or []),
                        "event_time": feature.name == fg.event_time
                    })
                
                # Get statistics config if available
                statistics_config = None
                if hasattr(fg, 'statistics_config'):
                    statistics_config = fg.statistics_config
                
                return {
                    "name": fg.name,
                    "version": fg.version,
                    "description": fg.description,
                    "primary_key": fg.primary_key or [],
                    "partition_key": fg.partition_key or [],
                    "event_time": fg.event_time,
                    "online_enabled": fg.online_enabled,
                    "time_travel_format": fg.time_travel_format,
                    "created": str(fg.created) if fg.created else None,
                    "creator": fg.creator,
                    "id": fg.id,
                    "features": features,
                    "statistics_config": statistics_config,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature group: {str(e)}"
                }
                
        @self.mcp.tool()
        async def list_feature_groups(
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List feature groups in a Hopsworks project's feature store.
            
            Args:
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                list: List of feature groups in the feature store
            """
            if ctx:
                await ctx.info(f"Listing feature groups for project: {project_name or 'default'}")
                
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                feature_groups = fs.get_feature_groups()
                
                # Convert to serializable format
                result = []
                for fg in feature_groups:
                    result.append({
                        "name": fg.name,
                        "version": fg.version,
                        "description": fg.description,
                        "created": str(fg.created) if fg.created else None,
                        "creator": fg.creator,
                        "online_enabled": fg.online_enabled,
                        "time_travel_format": fg.time_travel_format,
                        "primary_key": fg.primary_key or []
                    })
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list feature groups: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def get_feature_group_by_id(
            id: int,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a feature group entity from the feature store by ID.
            
            Args:
                id: ID of the feature group to get
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature group details
            """
            if ctx:
                await ctx.info(f"Getting feature group by ID: {id} from project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get all feature groups and find the one with the matching ID
                feature_groups = fs.get_feature_groups()
                matching_fg = None
                
                for fg in feature_groups:
                    if fg.id == id:
                        matching_fg = fg
                        break
                
                if matching_fg is None:
                    return {
                        "status": "error",
                        "message": f"Feature group with ID {id} not found"
                    }
                
                # Get features in a serializable format
                features = []
                for feature in matching_fg.features:
                    features.append({
                        "name": feature.name,
                        "type": feature.type,
                        "description": feature.description,
                        "primary": feature.name in (matching_fg.primary_key or []),
                        "partition": feature.name in (matching_fg.partition_key or []),
                        "event_time": feature.name == matching_fg.event_time
                    })
                
                return {
                    "name": matching_fg.name,
                    "version": matching_fg.version,
                    "description": matching_fg.description,
                    "primary_key": matching_fg.primary_key or [],
                    "partition_key": matching_fg.partition_key or [],
                    "event_time": matching_fg.event_time,
                    "online_enabled": matching_fg.online_enabled,
                    "time_travel_format": matching_fg.time_travel_format,
                    "created": str(matching_fg.created) if matching_fg.created else None,
                    "creator": matching_fg.creator,
                    "id": matching_fg.id,
                    "features": features,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature group by ID: {str(e)}"
                }
        
        @self.mcp.tool()
        async def read_feature_group(
            name: str,
            version: int = 1,
            limit: int = 100,
            online: bool = False,
            project_name: Optional[str] = None,
            time_travel: Optional[str] = None,
            dataframe_type: str = "pandas",
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Read data from a feature group into a result set.
            
            Args:
                name: Name of the feature group to read
                version: Version of the feature group (defaults to 1)
                limit: Maximum number of rows to return
                online: Whether to read from online feature store (defaults to False, which reads from offline store)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                time_travel: If specified, read data as of this point in time (format: YYYY-MM-DD HH:MM:SS)
                dataframe_type: Type of dataframe to use (pandas or spark)
                
            Returns:
                dict: Feature group data
            """
            if ctx:
                await ctx.info(f"Reading feature group: {name} (v{version}) from {'online' if online else 'offline'} store")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Read data from feature group
                if time_travel:
                    # Read with time travel
                    df = fg.read(wallclock_time=time_travel, online=online, dataframe_type=dataframe_type)
                else:
                    # Read latest data
                    df = fg.read(online=online, dataframe_type=dataframe_type)
                
                # Convert result to JSON
                if dataframe_type == "spark":
                    # Spark dataframe
                    result_rows = df.limit(limit).toJSON().collect()
                    rows = [json.loads(row) for row in result_rows]
                else:
                    # Pandas dataframe
                    rows = json.loads(df.head(limit).to_json(orient="records"))
                
                feature_names = [f.name for f in fg.features]
                
                return {
                    "name": fg.name,
                    "version": fg.version,
                    "rows": rows,
                    "features": feature_names,
                    "count": len(rows),
                    "truncated": len(rows) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to read feature group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def update_feature_group_description(
            name: str,
            description: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the description of a feature group.
            
            Args:
                name: Name of the feature group to update
                description: New description for the feature group
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Updated feature group information
            """
            if ctx:
                await ctx.info(f"Updating description for feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Update description
                fg.update_description(description)
                
                return {
                    "name": fg.name,
                    "version": fg.version,
                    "description": fg.description,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update feature group description: {str(e)}"
                }
        
        @self.mcp.tool()
        async def update_feature_description(
            name: str,
            feature_name: str,
            description: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the description of a feature in a feature group.
            
            Args:
                name: Name of the feature group containing the feature
                feature_name: Name of the feature to update
                description: New description for the feature
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Updated feature information
            """
            if ctx:
                await ctx.info(f"Updating description for feature '{feature_name}' in feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Update feature description
                fg.update_feature_description(feature_name=feature_name, description=description)
                
                # Get updated feature
                feature = fg.get_feature(feature_name)
                
                return {
                    "feature_group": name,
                    "version": version,
                    "feature_name": feature.name,
                    "description": feature.description,
                    "type": feature.type,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update feature description: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_feature_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a feature group along with its feature data.
            
            Args:
                name: Name of the feature group to delete
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Delete feature group
                fg.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete feature group: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_feature_group_statistics(
            name: str,
            version: int = 1,
            computation_time: Optional[str] = None,
            feature_names: Optional[List[str]] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get statistics for a feature group.
            
            Args:
                name: Name of the feature group
                version: Version of the feature group (defaults to 1)
                computation_time: Date and time when statistics were computed (optional)
                feature_names: List of feature names for which to get statistics (optional)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Feature group statistics
            """
            if ctx:
                await ctx.info(f"Getting statistics for feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Get statistics
                stats = fg.get_statistics(computation_time=computation_time, feature_names=feature_names)
                
                if stats is None:
                    return {
                        "name": name,
                        "version": version,
                        "status": "not_found",
                        "message": "No statistics found for this feature group"
                    }
                
                # Convert statistics to serializable format
                result = {
                    "name": name,
                    "version": version,
                    "computation_time": str(stats.computation_time) if stats.computation_time else None,
                    "features": {},
                    "status": "success"
                }
                
                # Process feature statistics
                for feature_name, feature_stats in stats.feature_descriptive_statistics.items():
                    result["features"][feature_name] = {
                        "count": feature_stats.count,
                        "distinct_count": feature_stats.distinctCount,
                        "unique_count": feature_stats.uniqueCount,
                        "mean": feature_stats.mean,
                        "max": feature_stats.max,
                        "min": feature_stats.min,
                        "std_dev": feature_stats.stddev,
                        "completeness": feature_stats.completeness
                    }
                
                return result
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature group statistics: {str(e)}"
                }
                
        @self.mcp.tool()
        async def compute_feature_group_statistics(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            wallclock_time: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Compute statistics for a feature group.
            
            Args:
                name: Name of the feature group
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                wallclock_time: If specified, compute statistics for feature group as of specific point in time
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Computing statistics for feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Compute statistics
                stats = fg.compute_statistics(wallclock_time=wallclock_time)
                
                return {
                    "name": name,
                    "version": version,
                    "computation_time": str(stats.computation_time) if stats.computation_time else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to compute feature group statistics: {str(e)}"
                }
        
        @self.mcp.tool()
        async def update_statistics_config(
            name: str,
            statistics_config: Dict[str, Any],
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the statistics configuration for a feature group.
            
            Args:
                name: Name of the feature group
                statistics_config: New statistics configuration with the following options:
                    - enabled: Whether to compute statistics (True/False)
                    - correlations: Whether to compute correlations between features (True/False)
                    - histograms: Whether to compute histograms of feature values (True/False)
                    - exact_uniqueness: Whether to compute exact uniqueness statistics (True/False)
                    - columns: List of columns for which to compute statistics (empty list for all columns)
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Updated statistics configuration
            """
            if ctx:
                await ctx.info(f"Updating statistics config for feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Update statistics config
                fg.statistics_config = statistics_config
                fg.update_statistics_config()
                
                return {
                    "name": name,
                    "version": version,
                    "statistics_config": statistics_config,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update statistics configuration: {str(e)}"
                }
        
        @self.mcp.tool()
        async def insert(
            name: str,
            features_data: str,
            version: int = 1,
            project_name: Optional[str] = None,
            write_options: Optional[Dict[str, Any]] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Insert feature data into a feature group.
            
            This method allows you to insert a dataframe into an existing feature group.
            The dataframe must contain columns matching the feature group schema,
            including primary key, partition key, and event time columns.
            
            Args:
                name: Name of the feature group
                features_data: JSON string representation of the dataframe to insert
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                write_options: Options for feature group insertion (e.g. {"start_offline_materialization": False})
                
            Returns:
                dict: Information about the insertion operation
            """
            # Call insert_dataframe for implementation
            return await self.insert_dataframe(
                name=name,
                features_data=features_data,
                version=version,
                project_name=project_name,
                write_options=write_options,
                ctx=ctx
            )
            
        @self.mcp.tool()
        async def insert_dataframe(
            name: str,
            features_data: str,
            version: int = 1,
            project_name: Optional[str] = None,
            write_options: Optional[Dict[str, Any]] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Insert feature data into a feature group.
            
            This method allows you to insert a dataframe into an existing feature group.
            The dataframe must contain columns matching the feature group schema,
            including primary key, partition key, and event time columns.
            
            Args:
                name: Name of the feature group
                features_data: JSON string representation of the dataframe to insert
                version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                write_options: Options for feature group insertion (e.g. {"start_offline_materialization": False})
                
            Returns:
                dict: Information about the insertion operation
            """
            if ctx:
                await ctx.info(f"Inserting data into feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                fg = fs.get_feature_group(name=name, version=version)
                
                # Convert JSON string to pandas DataFrame
                import pandas as pd
                import json
                
                df = pd.read_json(features_data, orient='records')
                
                # Set default write options if not provided
                if write_options is None:
                    write_options = {}
                
                # Insert data into feature group
                job, validation_report = fg.insert(df, write_options=write_options)
                
                # Determine insertion status
                status = "success"
                if job and hasattr(job, 'state'):
                    job_status = job.state()
                    status = job_status.upper() if job_status else "SUBMITTED"
                
                # Return results
                return {
                    "name": name,
                    "version": version,
                    "rows_inserted": len(df),
                    "validation": True if validation_report else False,
                    "job_id": job.id if job and hasattr(job, 'id') else None,
                    "job_name": job.name if job and hasattr(job, 'name') else None,
                    "job_status": status,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to insert data into feature group: {str(e)}"
                }