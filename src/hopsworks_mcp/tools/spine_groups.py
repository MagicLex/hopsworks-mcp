"""Spine groups capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union
import json
import hopsworks
from fastmcp import Context


class SpineGroupTools:
    """Tools for working with Hopsworks spine groups."""

    def __init__(self, mcp):
        """Initialize spine group tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def get_or_create_spine_group(
            name: str,
            data: str,  # JSON string
            primary_key: List[str],
            version: Optional[int] = None,
            description: str = "",
            event_time: Optional[str] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create or retrieve a spine group with the provided data.
            
            A spine group is a metadata object similar to a feature group, but the data 
            is not materialized in the feature store. It contains the needed metadata
            such as the event time and primary key columns to perform point-in-time correct joins.
            
            Args:
                name: Name of the spine group
                data: JSON string representation of the data for the spine
                primary_key: List of feature names to use as primary key
                version: Version of the spine group (defaults to incremented from last)
                description: Description of the spine group
                event_time: Name of the feature containing event time
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Spine group information
            """
            if ctx:
                await ctx.info(f"Creating spine group: {name}")
            
            try:
                import pandas as pd
                
                # Parse JSON data to DataFrame
                df = pd.read_json(data, orient='records')
                
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Create spine group
                spine_group = fs.get_or_create_spine_group(
                    name=name,
                    version=version,
                    description=description,
                    primary_key=primary_key,
                    event_time=event_time,
                    dataframe=df
                )
                
                # Return metadata about the spine group
                return {
                    "name": spine_group.name,
                    "version": spine_group.version,
                    "description": spine_group.description,
                    "primary_key": spine_group.primary_key if hasattr(spine_group, 'primary_key') else None,
                    "event_time": spine_group.event_time if hasattr(spine_group, 'event_time') else None,
                    "rows_count": len(df),
                    "features": [{"name": f.name, "type": f.type} for f in spine_group.schema] if hasattr(spine_group, 'schema') else [],
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create spine group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_spine_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get a spine group and its metadata.
            
            Args:
                name: Name of the spine group to retrieve
                version: Version of the spine group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Spine group information
            """
            if ctx:
                await ctx.info(f"Getting spine group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get spine group
                spine_group = fs.get_or_create_spine_group(
                    name=name,
                    version=version
                )
                
                # Get a sample of the data
                sample = []
                if hasattr(spine_group, 'dataframe'):
                    try:
                        df_sample = spine_group.dataframe.limit(5).toPandas() if hasattr(spine_group.dataframe, 'limit') else spine_group.dataframe.head(5)
                        sample = json.loads(df_sample.to_json(orient='records'))
                    except:
                        pass
                
                # Return metadata about the spine group
                return {
                    "name": spine_group.name,
                    "version": spine_group.version,
                    "description": spine_group.description if hasattr(spine_group, 'description') else None,
                    "primary_key": spine_group.primary_key if hasattr(spine_group, 'primary_key') else None,
                    "event_time": spine_group.event_time if hasattr(spine_group, 'event_time') else None,
                    "features": [{"name": f.name, "type": f.type} for f in spine_group.schema] if hasattr(spine_group, 'schema') else [],
                    "sample_data": sample,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get spine group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def update_spine_group_data(
            name: str,
            data: str,  # JSON string
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the data in a spine group.
            
            Args:
                name: Name of the spine group to update
                data: JSON string representation of the new data for the spine
                version: Version of the spine group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Updating data in spine group: {name} (v{version})")
            
            try:
                import pandas as pd
                
                # Parse JSON data to DataFrame
                df = pd.read_json(data, orient='records')
                
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get spine group
                spine_group = fs.get_or_create_spine_group(
                    name=name,
                    version=version
                )
                
                # Update dataframe
                spine_group.dataframe = df
                
                return {
                    "name": spine_group.name,
                    "version": spine_group.version,
                    "rows_count": len(df),
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update spine group data: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_spine_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a spine group.
            
            Args:
                name: Name of the spine group to delete
                version: Version of the spine group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting spine group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get spine group
                spine_group = fs.get_or_create_spine_group(
                    name=name,
                    version=version
                )
                
                # Delete spine group
                spine_group.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete spine group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_feature_view_with_spine(
            name: str,
            version: int = 1,
            spine_group_name: str = None,
            spine_group_version: int = 1,
            feature_group_name: str = None,
            feature_group_version: int = 1,
            join_key: List[str] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a feature view using a spine group.
            
            This creates a feature view that combines a spine group (containing labels or 
            target variables) with a feature group (containing features).
            
            Args:
                name: Name of the feature view to create
                version: Version of the feature view
                spine_group_name: Name of the spine group to use
                spine_group_version: Version of the spine group
                feature_group_name: Name of the feature group to join with
                feature_group_version: Version of the feature group
                join_key: List of columns to join on
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Feature view information
            """
            if ctx:
                await ctx.info(f"Creating feature view with spine group: {spine_group_name} and feature group: {feature_group_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get spine group
                spine_group = fs.get_or_create_spine_group(
                    name=spine_group_name,
                    version=spine_group_version
                )
                
                # Get feature group
                feature_group = fs.get_feature_group(
                    name=feature_group_name,
                    version=feature_group_version
                )
                
                # Create query
                if not join_key and hasattr(spine_group, 'primary_key') and spine_group.primary_key:
                    join_key = spine_group.primary_key
                
                query = spine_group.select_all().join(
                    feature_group.select_all(),
                    on=join_key
                )
                
                # Create feature view
                feature_view = fs.create_feature_view(
                    name=name,
                    version=version,
                    query=query
                )
                
                # Save the feature view
                feature_view.save()
                
                return {
                    "name": feature_view.name,
                    "version": feature_view.version,
                    "spine_group": spine_group_name,
                    "spine_group_version": spine_group_version,
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "join_key": join_key,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create feature view with spine: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_batch_data_with_spine(
            feature_view_name: str,
            spine_data: str,  # JSON string
            feature_view_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get batch data from a feature view using a spine.
            
            This allows getting feature data for a specific set of entities defined
            in the spine data.
            
            Args:
                feature_view_name: Name of the feature view
                spine_data: JSON string representation of the spine data
                feature_view_version: Version of the feature view
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Batch data results
            """
            if ctx:
                await ctx.info(f"Getting batch data from feature view: {feature_view_name} using spine data")
            
            try:
                import pandas as pd
                
                # Parse JSON data to DataFrame
                spine_df = pd.read_json(spine_data, orient='records')
                
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature view
                feature_view = fs.get_feature_view(
                    name=feature_view_name,
                    version=feature_view_version
                )
                
                # Get batch data using spine
                batch_df = feature_view.get_batch_data(spine=spine_df)
                
                # Convert to JSON for response
                batch_data = json.loads(batch_df.head(100).to_json(orient='records'))
                
                return {
                    "feature_view": feature_view_name,
                    "feature_view_version": feature_view_version,
                    "rows_count": len(batch_df),
                    "batch_data": batch_data,
                    "truncated": len(batch_df) > 100,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get batch data with spine: {str(e)}"
                }
        
        @self.mcp.tool()
        async def create_train_test_split_with_spine(
            feature_view_name: str,
            spine_data: str,  # JSON string
            test_size: float = 0.2,
            feature_view_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a train-test split using a spine for a feature view.
            
            This allows creating training and testing datasets for specific entities
            defined in the spine data.
            
            Args:
                feature_view_name: Name of the feature view
                spine_data: JSON string representation of the spine data
                test_size: Fraction of data to use for testing (0.0 to 1.0)
                feature_view_version: Version of the feature view
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Train-test split information
            """
            if ctx:
                await ctx.info(f"Creating train-test split for feature view: {feature_view_name} using spine data")
            
            try:
                import pandas as pd
                
                # Parse JSON data to DataFrame
                spine_df = pd.read_json(spine_data, orient='records')
                
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature view
                feature_view = fs.get_feature_view(
                    name=feature_view_name,
                    version=feature_view_version
                )
                
                # Create train-test split using spine
                X_train, X_test, y_train, y_test = feature_view.train_test_split(
                    test_size=test_size,
                    spine=spine_df
                )
                
                return {
                    "feature_view": feature_view_name,
                    "feature_view_version": feature_view_version,
                    "test_size": test_size,
                    "X_train_shape": (X_train.shape[0], X_train.shape[1]) if hasattr(X_train, 'shape') else None,
                    "X_test_shape": (X_test.shape[0], X_test.shape[1]) if hasattr(X_test, 'shape') else None,
                    "y_train_shape": (y_train.shape[0], y_train.shape[1]) if hasattr(y_train, 'shape') and len(y_train.shape) > 1 else (y_train.shape[0], 1) if hasattr(y_train, 'shape') else None,
                    "y_test_shape": (y_test.shape[0], y_test.shape[1]) if hasattr(y_test, 'shape') and len(y_test.shape) > 1 else (y_test.shape[0], 1) if hasattr(y_test, 'shape') else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create train-test split with spine: {str(e)}"
                }