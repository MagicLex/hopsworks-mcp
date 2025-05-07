"""External feature groups capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union
import hopsworks
from fastmcp import Context


class ExternalFeatureGroupTools:
    """Tools for working with Hopsworks External Feature Groups."""

    def __init__(self, mcp):
        """Initialize external feature group tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_external_feature_group(
            name: str,
            storage_connector: str,
            query: Optional[str] = None,
            data_format: Optional[str] = None, 
            path: Optional[str] = None,
            options: Optional[Dict[str, str]] = None,
            version: Optional[int] = None,
            description: str = "",
            primary_key: Optional[List[str]] = None,
            foreign_key: Optional[List[str]] = None,
            event_time: Optional[str] = None, 
            online_enabled: bool = False,
            topic_name: Optional[str] = None,
            notification_topic_name: Optional[str] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create an external feature group metadata object and save it to Hopsworks.
            
            An external feature group contains metadata about feature data in an external 
            storage system such as Snowflake, Redshift, or S3.
            
            Args:
                name: Name of the external feature group to create
                storage_connector: Name of the storage connector to use
                query: SQL query valid for the target data source (required for JDBC connectors)
                data_format: Data format to use when reading from file-based storage
                path: Location within the storage connector to read data from
                options: Additional options for the storage engine
                version: Version of the feature group
                description: Description of the feature group
                primary_key: List of feature names to use as primary key
                foreign_key: List of feature names to use as foreign key
                event_time: Name of the feature containing event time
                online_enabled: Whether to enable online storage
                topic_name: Name of the Kafka topic for data ingestion
                notification_topic_name: Name of the Kafka topic for notifications
                project_name: Name of the Hopsworks project (defaults to current project)
                
            Returns:
                dict: External feature group information
            """
            if ctx:
                await ctx.info(f"Creating external feature group: {name} (v{version or 'auto'}) in project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # First, get the storage connector
                connector = None
                for conn in project.get_storage_connectors():
                    if conn.name == storage_connector:
                        connector = conn
                        break
                
                if not connector:
                    return {
                        "status": "error",
                        "message": f"Storage connector '{storage_connector}' not found"
                    }
                
                # Create the feature group
                external_fg = fs.create_external_feature_group(
                    name=name,
                    storage_connector=connector,
                    query=query,
                    data_format=data_format,
                    path=path if path else "",
                    options=options,
                    version=version,
                    description=description,
                    primary_key=primary_key,
                    foreign_key=foreign_key,
                    event_time=event_time,
                    online_enabled=online_enabled,
                    topic_name=topic_name,
                    notification_topic_name=notification_topic_name
                )
                
                # Save the feature group
                external_fg.save()
                
                return {
                    "name": external_fg.name,
                    "version": external_fg.version,
                    "description": external_fg.description,
                    "primary_key": external_fg.primary_key if hasattr(external_fg, 'primary_key') else None,
                    "event_time": external_fg.event_time if hasattr(external_fg, 'event_time') else None,
                    "online_enabled": external_fg.online_enabled if hasattr(external_fg, 'online_enabled') else False,
                    "feature_store_name": external_fg.feature_store_name if hasattr(external_fg, 'feature_store_name') else None,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create external feature group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_external_feature_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get an external feature group entity from the feature store.
            
            Args:
                name: Name of the external feature group to retrieve
                version: Version of the external feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: External feature group details
            """
            if ctx:
                await ctx.info(f"Getting external feature group: {name} (v{version}) from project {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                external_fg = fs.get_external_feature_group(name=name, version=version)
                
                # Get schema information
                schema = []
                if hasattr(external_fg, 'schema'):
                    for feature in external_fg.schema:
                        schema.append({
                            "name": feature.name,
                            "type": feature.type,
                            "description": feature.description
                        })
                
                return {
                    "name": external_fg.name,
                    "version": external_fg.version,
                    "description": external_fg.description,
                    "primary_key": external_fg.primary_key if hasattr(external_fg, 'primary_key') else None,
                    "schema": schema,
                    "event_time": external_fg.event_time if hasattr(external_fg, 'event_time') else None,
                    "online_enabled": external_fg.online_enabled if hasattr(external_fg, 'online_enabled') else False,
                    "feature_store_name": external_fg.feature_store_name if hasattr(external_fg, 'feature_store_name') else None,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get external feature group: {str(e)}"
                }
                
        @self.mcp.tool()
        async def list_external_feature_groups(
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List external feature groups in a Hopsworks project's feature store.
            
            Args:
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                list: List of external feature groups in the feature store
            """
            if ctx:
                await ctx.info(f"Listing external feature groups for project: {project_name or 'default'}")
                
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # There's no direct method to get all external feature groups,
                # so we need to use SQL to query metadata
                query = "SELECT name, version FROM feature_store_metadata.feature_group WHERE feature_group_type='ON_DEMAND_FEATURE_GROUP'"
                df = fs.sql(query, dataframe_type="pandas")
                
                result = []
                for _, row in df.iterrows():
                    try:
                        fg = fs.get_external_feature_group(name=row['name'], version=row['version'])
                        result.append({
                            "name": fg.name,
                            "version": fg.version,
                            "description": fg.description if hasattr(fg, 'description') else "",
                            "primary_key": fg.primary_key if hasattr(fg, 'primary_key') else None,
                            "online_enabled": fg.online_enabled if hasattr(fg, 'online_enabled') else False,
                            "feature_store_name": fg.feature_store_name if hasattr(fg, 'feature_store_name') else None
                        })
                    except:
                        # Skip any feature groups that can't be retrieved
                        pass
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list external feature groups: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def delete_external_feature_group(
            name: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete an external feature group metadata.
            
            Args:
                name: Name of the external feature group to delete
                version: Version of the external feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting external feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                external_fg = fs.get_external_feature_group(name=name, version=version)
                
                # Delete feature group
                external_fg.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete external feature group: {str(e)}"
                }
                
        @self.mcp.tool()
        async def update_external_feature_group_description(
            name: str,
            description: str,
            version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the description of an external feature group.
            
            Args:
                name: Name of the external feature group to update
                description: New description for the feature group
                version: Version of the external feature group (defaults to 1)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Updated external feature group information
            """
            if ctx:
                await ctx.info(f"Updating description for external feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                external_fg = fs.get_external_feature_group(name=name, version=version)
                
                # Update description
                external_fg.description = description
                external_fg.update_description(description)
                
                return {
                    "name": external_fg.name,
                    "version": external_fg.version,
                    "description": external_fg.description,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update external feature group description: {str(e)}"
                }
        
        @self.mcp.tool()
        async def insert_into_online_store(
            name: str,
            feature_data: str,
            version: int = 1,
            wait: bool = False,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Insert feature data into the online feature store.
            
            For external feature groups, the online store must be manually populated
            by providing feature data (serialized as a JSON string).
            
            Args:
                name: Name of the external feature group 
                feature_data: JSON string representation of the feature data
                version: Version of the external feature group (defaults to 1)
                wait: Whether to wait for ingestion to complete (defaults to False)
                project_name: Name of the Hopsworks project's feature store (defaults to current project)
                
            Returns:
                dict: Information about the ingestion
            """
            if ctx:
                await ctx.info(f"Inserting data into online store for external feature group: {name} (v{version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                external_fg = fs.get_external_feature_group(name=name, version=version)
                
                # Check if online_enabled is True
                if not external_fg.online_enabled:
                    return {
                        "status": "error",
                        "message": f"External feature group {name} (v{version}) is not enabled for online storage"
                    }
                
                # Convert JSON string to pandas DataFrame
                import pandas as pd
                import json
                
                df = pd.read_json(feature_data, orient='records')
                
                # Insert into online store
                validation_report = external_fg.insert(
                    features=df, 
                    wait=wait
                )
                
                return {
                    "name": external_fg.name,
                    "version": external_fg.version,
                    "rows_inserted": len(df),
                    "validation": True if validation_report else False,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to insert data into online store: {str(e)}"
                }