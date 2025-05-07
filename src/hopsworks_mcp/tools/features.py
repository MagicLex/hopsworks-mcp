"""Features capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union
import hopsworks
from fastmcp import Context


class FeatureTools:
    """Tools for working with individual Hopsworks features."""

    def __init__(self, mcp):
        """Initialize feature tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_feature(
            name: str,
            type: str,
            description: Optional[str] = None,
            primary: bool = False,
            foreign: bool = False,
            partition: bool = False,
            hudi_precombine_key: bool = False,
            online_type: Optional[str] = None,
            default_value: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a feature definition that can be used in feature groups.
            
            Args:
                name: Name of the feature
                type: Data type in offline feature store (e.g., 'string', 'int', 'float')
                description: Description of the feature
                primary: Whether this feature is part of primary key
                foreign: Whether this feature is part of foreign key
                partition: Whether this feature is part of partition key 
                hudi_precombine_key: Whether this feature is hudi precombine key
                online_type: Data type in online feature store
                default_value: Default value for the feature
                
            Returns:
                dict: Feature information
            """
            if ctx:
                await ctx.info(f"Creating feature definition: {name} ({type})")
            
            try:
                import hsfs
                
                # Create feature definition
                feature = hsfs.feature.Feature(
                    name=name,
                    type=type,
                    description=description,
                    primary=primary,
                    foreign=foreign,
                    partition=partition,
                    hudi_precombine_key=hudi_precombine_key,
                    online_type=online_type,
                    default_value=default_value
                )
                
                # Return the feature properties (can't return the feature object directly)
                return {
                    "name": feature.name,
                    "type": feature.type,
                    "description": feature.description,
                    "primary": feature.primary,
                    "foreign": feature.foreign,
                    "partition": feature.partition,
                    "hudi_precombine_key": feature.hudi_precombine_key,
                    "online_type": feature.online_type,
                    "default_value": feature.default_value,
                    "is_complex": feature.is_complex(),
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create feature: {str(e)}"
                }
        
        @self.mcp.tool()
        async def update_feature_description(
            feature_group_name: str,
            feature_name: str,
            description: str,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Update the description of a feature in a feature group.
            
            Args:
                feature_group_name: Name of the feature group containing the feature
                feature_name: Name of the feature to update
                description: New description for the feature
                feature_group_version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project (defaults to current project)
                
            Returns:
                dict: Updated feature information
            """
            if ctx:
                await ctx.info(f"Updating description for feature: {feature_name} in feature group: {feature_group_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Get feature and update its description
                feature = fg.get_feature(feature_name)
                if not feature:
                    return {
                        "status": "error", 
                        "message": f"Feature {feature_name} not found in feature group {feature_group_name}"
                    }
                
                # Update feature description
                fg.update_feature_description(feature_name=feature_name, description=description)
                
                # Get the updated feature
                updated_feature = fg.get_feature(feature_name)
                
                return {
                    "name": updated_feature.name,
                    "type": updated_feature.type if hasattr(updated_feature, 'type') else None,
                    "description": updated_feature.description,
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "status": "updated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to update feature description: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_feature_info(
            feature_group_name: str,
            feature_name: str,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get information about a specific feature in a feature group.
            
            Args:
                feature_group_name: Name of the feature group containing the feature
                feature_name: Name of the feature to get
                feature_group_version: Version of the feature group (defaults to 1)
                project_name: Name of the Hopsworks project (defaults to current project)
                
            Returns:
                dict: Feature information
            """
            if ctx:
                await ctx.info(f"Getting info for feature: {feature_name} in feature group: {feature_group_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Get feature
                feature = fg.get_feature(feature_name)
                if not feature:
                    return {
                        "status": "error", 
                        "message": f"Feature {feature_name} not found in feature group {feature_group_name}"
                    }
                
                return {
                    "name": feature.name,
                    "type": feature.type if hasattr(feature, 'type') else None,
                    "description": feature.description if hasattr(feature, 'description') else None,
                    "primary": feature.primary if hasattr(feature, 'primary') else False,
                    "foreign": feature.foreign if hasattr(feature, 'foreign') else False,
                    "partition": feature.partition if hasattr(feature, 'partition') else False,
                    "online_type": feature.online_type if hasattr(feature, 'online_type') else None,
                    "default_value": feature.default_value if hasattr(feature, 'default_value') else None,
                    "is_complex": feature.is_complex() if hasattr(feature, 'is_complex') else None,
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature info: {str(e)}"
                }
                
        @self.mcp.tool()
        async def search_features(
            pattern: str,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """Search for features across all feature groups that match a pattern.
            
            Args:
                pattern: Name pattern to search for (uses SQL LIKE syntax, e.g., '%sales%')
                project_name: Name of the Hopsworks project (defaults to current project)
                
            Returns:
                list: Matching feature information
            """
            if ctx:
                await ctx.info(f"Searching for features matching pattern: {pattern}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Use SQL to search across all feature groups
                # This includes both regular and external feature groups
                query = f"""
                SELECT fg.name as fg_name, fg.version as fg_version, f.name as f_name, 
                       f.description as f_description, f.type as f_type,
                       f.primary as f_primary, f.partition as f_partition 
                FROM feature_store_metadata.feature f
                JOIN feature_store_metadata.feature_group fg ON f.feature_group_id = fg.id
                WHERE f.name LIKE '{pattern}'
                ORDER BY fg.name, fg.version, f.name
                """
                
                df = fs.sql(query, dataframe_type="pandas")
                
                results = []
                for _, row in df.iterrows():
                    results.append({
                        "feature_name": row['f_name'],
                        "feature_description": row['f_description'],
                        "feature_type": row['f_type'],
                        "feature_group_name": row['fg_name'],
                        "feature_group_version": row['fg_version'],
                        "is_primary": row['f_primary'] == 1,
                        "is_partition": row['f_partition'] == 1
                    })
                
                return results
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to search features: {str(e)}"
                }]
                
        @self.mcp.tool()
        async def list_feature_statistics(
            feature_group_name: str,
            feature_name: str,
            feature_group_version: int = 1,
            limit: int = 5,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get statistical information about a specific feature.
            
            Args:
                feature_group_name: Name of the feature group containing the feature
                feature_name: Name of the feature to get statistics for
                feature_group_version: Version of the feature group (defaults to 1)
                limit: Maximum number of computation times to return stats for
                project_name: Name of the Hopsworks project (defaults to current project)
                
            Returns:
                dict: Feature statistics information
            """
            if ctx:
                await ctx.info(f"Getting statistics for feature: {feature_name} in feature group: {feature_group_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Get all statistics
                all_stats = fg.get_all_statistics(feature_names=[feature_name])
                if not all_stats or len(all_stats) == 0:
                    return {
                        "status": "success", 
                        "message": f"No statistics found for feature {feature_name}"
                    }
                
                # Sort by computation time (latest first) and limit
                sorted_stats = sorted(all_stats, 
                                      key=lambda x: x.computation_time if hasattr(x, 'computation_time') else 0, 
                                      reverse=True)
                stats_to_return = sorted_stats[:limit]
                
                # Extract feature-specific statistics
                result = {
                    "feature_name": feature_name,
                    "feature_group_name": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "statistics": []
                }
                
                for stat in stats_to_return:
                    if not hasattr(stat, 'feature_descriptive_statistics'):
                        continue
                        
                    for feat_stat in stat.feature_descriptive_statistics:
                        if feat_stat.feature_name == feature_name:
                            stat_entry = {
                                "computation_time": str(stat.computation_time) if hasattr(stat, 'computation_time') else None,
                                "count": feat_stat.count if hasattr(feat_stat, 'count') else None,
                                "mean": feat_stat.mean if hasattr(feat_stat, 'mean') else None,
                                "min": feat_stat.min if hasattr(feat_stat, 'min') else None,
                                "max": feat_stat.max if hasattr(feat_stat, 'max') else None,
                                "std": feat_stat.stddev if hasattr(feat_stat, 'stddev') else None,
                                "completeness": feat_stat.completeness if hasattr(feat_stat, 'completeness') else None,
                                "approx_unique": feat_stat.approx_num_distinct if hasattr(feat_stat, 'approx_num_distinct') else None
                            }
                            result["statistics"].append(stat_entry)
                            break
                
                result["status"] = "success"
                return result
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get feature statistics: {str(e)}"
                }