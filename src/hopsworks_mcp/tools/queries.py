"""Queries capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union, Literal
import json
import hopsworks
from fastmcp import Context


class QueryTools:
    """Tools for working with Hopsworks queries across feature groups."""

    def __init__(self, mcp):
        """Initialize query tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def execute_sql_query(
            sql: str,
            project_name: Optional[str] = None,
            limit: int = 100,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Execute a SQL query directly against the feature store.
            
            Args:
                sql: SQL query to execute
                project_name: Name of the Hopsworks project
                limit: Maximum number of rows to return
                
            Returns:
                dict: Query results
            """
            if ctx:
                await ctx.info(f"Executing SQL query against feature store")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Execute SQL query
                df = fs.sql(sql, dataframe_type="pandas")
                
                # Limit results and convert to JSON
                results = json.loads(df.head(limit).to_json(orient="records"))
                
                return {
                    "results": results,
                    "rows_count": len(results),
                    "truncated": len(results) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to execute SQL query: {str(e)}"
                }
        
        @self.mcp.tool()
        async def join_feature_groups(
            feature_group1_name: str,
            feature_group2_name: str,
            feature_group1_version: int = 1,
            feature_group2_version: int = 1,
            on: Optional[List[str]] = None,
            left_on: Optional[List[str]] = None,
            right_on: Optional[List[str]] = None,
            join_type: Literal["inner", "left", "right", "outer"] = "inner",
            limit: int = 100,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Join two feature groups and return the results.
            
            Args:
                feature_group1_name: Name of the first feature group
                feature_group2_name: Name of the second feature group
                feature_group1_version: Version of the first feature group
                feature_group2_version: Version of the second feature group
                on: Common columns to join on (used if column names are the same)
                left_on: Columns from the left feature group to join on
                right_on: Columns from the right feature group to join on
                join_type: Type of join to perform (inner, left, right, outer)
                limit: Maximum number of rows to return
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Join results
            """
            if ctx:
                await ctx.info(f"Joining feature groups: {feature_group1_name} and {feature_group2_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature groups
                try:
                    fg1 = fs.get_feature_group(name=feature_group1_name, version=feature_group1_version)
                except:
                    # Try external feature group
                    fg1 = fs.get_external_feature_group(name=feature_group1_name, version=feature_group1_version)
                
                try:
                    fg2 = fs.get_feature_group(name=feature_group2_name, version=feature_group2_version)
                except:
                    # Try external feature group
                    fg2 = fs.get_external_feature_group(name=feature_group2_name, version=feature_group2_version)
                
                # Create query with join
                query = fg1.select_all().join(
                    fg2.select_all(),
                    on=on,
                    left_on=left_on,
                    right_on=right_on,
                    join_type=join_type.lower()
                )
                
                # Execute query
                df = query.read(dataframe_type="pandas")
                
                # Limit results and convert to JSON
                results = json.loads(df.head(limit).to_json(orient="records"))
                
                return {
                    "feature_group1": feature_group1_name,
                    "feature_group1_version": feature_group1_version,
                    "feature_group2": feature_group2_name,
                    "feature_group2_version": feature_group2_version,
                    "join_type": join_type,
                    "results": results,
                    "rows_count": len(results),
                    "truncated": len(results) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to join feature groups: {str(e)}"
                }
        
        @self.mcp.tool()
        async def filter_feature_group(
            feature_group_name: str,
            filter_expression: str,
            feature_group_version: int = 1,
            limit: int = 100,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Filter a feature group with a filter expression and return the results.
            
            Args:
                feature_group_name: Name of the feature group to filter
                filter_expression: Filter expression (e.g. "column1 > 10", "column2 == 'value'")
                feature_group_version: Version of the feature group
                limit: Maximum number of rows to return
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Filtered results
            """
            if ctx:
                await ctx.info(f"Filtering feature group {feature_group_name} with expression: {filter_expression}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature group
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # Try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Parse the filter expression
                # This is a simple parsing for basic filter expressions
                # Complex filters would need more sophisticated parsing
                try:
                    # Format: column operator value
                    # e.g., "id > 10" or "category == 'books'"
                    parts = filter_expression.split()
                    if len(parts) >= 3:
                        col = parts[0]
                        op = parts[1]
                        val = ' '.join(parts[2:])
                        
                        # Remove quotes if present
                        if val.startswith("'") and val.endswith("'"):
                            val = val[1:-1]
                        if val.startswith('"') and val.endswith('"'):
                            val = val[1:-1]
                        
                        # Convert to appropriate type if needed
                        try:
                            if '.' in val:
                                val = float(val)
                            else:
                                val = int(val)
                        except:
                            pass  # Keep as string
                        
                        # Construct filter
                        if op == '==':
                            filter_obj = (getattr(fg, col) == val)
                        elif op == '>':
                            filter_obj = (getattr(fg, col) > val)
                        elif op == '<':
                            filter_obj = (getattr(fg, col) < val)
                        elif op == '>=':
                            filter_obj = (getattr(fg, col) >= val)
                        elif op == '<=':
                            filter_obj = (getattr(fg, col) <= val)
                        elif op.lower() == 'like':
                            filter_obj = getattr(fg, col).like(val)
                        else:
                            raise ValueError(f"Unsupported operator: {op}")
                        
                        # Create and execute filtered query
                        query = fg.select_all().filter(filter_obj)
                        df = query.read(dataframe_type="pandas")
                        
                        # Limit results and convert to JSON
                        results = json.loads(df.head(limit).to_json(orient="records"))
                        
                        return {
                            "feature_group": feature_group_name,
                            "feature_group_version": feature_group_version,
                            "filter_expression": filter_expression,
                            "results": results,
                            "rows_count": len(results),
                            "truncated": len(results) >= limit,
                            "status": "success"
                        }
                    else:
                        raise ValueError("Invalid filter expression format")
                except Exception as filter_error:
                    return {
                        "status": "error",
                        "message": f"Failed to parse filter expression: {str(filter_error)}"
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to filter feature group: {str(e)}"
                }
        
        @self.mcp.tool()
        async def time_travel_query(
            feature_group_name: str,
            as_of_time: str,
            exclude_until_time: Optional[str] = None,
            feature_group_version: int = 1,
            limit: int = 100,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Execute a time travel query on a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                as_of_time: Point in time to query (format: YYYY-MM-DD HH:MM:SS)
                exclude_until_time: Exclude commits until this time (optional)
                feature_group_version: Version of the feature group
                limit: Maximum number of rows to return
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Query results at the specified point in time
            """
            if ctx:
                await ctx.info(f"Executing time travel query on {feature_group_name} as of {as_of_time}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get feature group (only regular feature groups support time travel)
                fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Create time travel query
                query = fg.select_all().as_of(
                    wallclock_time=as_of_time,
                    exclude_until=exclude_until_time
                )
                
                # Execute query
                df = query.read(dataframe_type="pandas")
                
                # Limit results and convert to JSON
                results = json.loads(df.head(limit).to_json(orient="records"))
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "as_of_time": as_of_time,
                    "exclude_until_time": exclude_until_time,
                    "results": results,
                    "rows_count": len(results),
                    "truncated": len(results) >= limit,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to execute time travel query: {str(e)}"
                }
        
        @self.mcp.tool()
        async def analyze_query_schema(
            query_expression: str,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Analyze the schema of a complex query without executing it.
            
            The query expression should be a series of operations on feature groups
            that can be evaluated to construct a query object. This is useful to
            understand what columns would be available in a complex query.
            
            Example query expressions:
            - fg1.select_all().join(fg2.select(['col1', 'col2']), on=['id'])
            - fg1.select_all().filter(fg1.col1 > 10)
            
            Args:
                query_expression: Python-like expression to construct the query
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Query schema information
            """
            if ctx:
                await ctx.info(f"Analyzing schema of query: {query_expression}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Parse and execute the query expression to get a query object
                # This is highly simplified - a real implementation would need
                # a more robust approach to evaluate expressions safely
                
                # Define a local namespace with access to feature store and feature groups
                local_ns = {"fs": fs}
                
                # Get all feature groups to make them available in the namespace
                feature_groups = await ctx.info("Getting all feature groups...")
                feature_groups_query = "SELECT name, version FROM feature_store_metadata.feature_group"
                fg_df = fs.sql(feature_groups_query, dataframe_type="pandas")
                
                for _, row in fg_df.iterrows():
                    name = row['name']
                    version = row['version']
                    fg_var_name = name.replace("-", "_").replace(" ", "_").lower()
                    try:
                        # Try regular feature group
                        local_ns[fg_var_name] = fs.get_feature_group(name=name, version=version)
                    except:
                        try:
                            # Try external feature group
                            local_ns[fg_var_name] = fs.get_external_feature_group(name=name, version=version)
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
                
                # Extract schema information
                features = []
                for feature in query.features:
                    features.append({
                        "name": feature.name,
                        "type": feature.type if hasattr(feature, 'type') else 'unknown',
                        "primary": feature.primary if hasattr(feature, 'primary') else False,
                        "partition": feature.partition if hasattr(feature, 'partition') else False
                    })
                
                feature_groups = []
                for fg in query.featuregroups:
                    feature_groups.append({
                        "name": fg.name,
                        "version": fg.version
                    })
                
                # Get information about joins if present
                joins = []
                if hasattr(query, 'joins') and query.joins:
                    for join in query.joins:
                        join_info = {
                            "join_type": join.join_type if hasattr(join, 'join_type') else 'inner',
                        }
                        if hasattr(join, 'on') and join.on:
                            join_info["on"] = join.on
                        if hasattr(join, 'left_on') and join.left_on:
                            join_info["left_on"] = join.left_on
                        if hasattr(join, 'right_on') and join.right_on:
                            join_info["right_on"] = join.right_on
                        joins.append(join_info)
                
                # Check for ambiguous features
                ambiguous_features = {}
                if hasattr(query, 'get_ambiguous_features'):
                    ambiguous_features = query.get_ambiguous_features()
                
                return {
                    "features": features,
                    "feature_groups": feature_groups,
                    "joins": joins,
                    "ambiguous_features": ambiguous_features,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to analyze query schema: {str(e)}"
                }