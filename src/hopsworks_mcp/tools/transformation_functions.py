"""Transformation functions capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union, Callable, Literal
from fastmcp import Context
import hopsworks
import inspect
import json


class TransformationFunctionsTools:
    """Tools for working with Hopsworks transformation functions."""

    def __init__(self, mcp):
        """Initialize transformation functions tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_transformation_function(
            transformation_function_code: str,
            return_types: Union[str, List[str]],
            name: Optional[str] = None,
            execution_mode: Literal["default", "python", "pandas"] = "default",
            version: Optional[int] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a new transformation function for feature transformations.
            
            Args:
                transformation_function_code: Python code of the transformation function
                return_types: Return type(s) of the function (e.g., 'float', ['float', 'int'])
                name: Optional name for the transformation function (defaults to function name)
                execution_mode: Mode for function execution ('default', 'python', 'pandas')
                version: Optional version of the transformation function
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Information about the created transformation function
            """
            if ctx:
                await ctx.info(f"Creating transformation function: {name or 'unnamed'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Convert string return_types to list if needed
                if isinstance(return_types, str):
                    return_types_list = [return_types]
                else:
                    return_types_list = return_types
                
                # Map string types to Python types
                type_mapping = {
                    "int": int,
                    "float": float,
                    "str": str,
                    "bool": bool,
                    "string": str,
                    "integer": int,
                    "boolean": bool
                }
                
                python_return_types = []
                for rt in return_types_list:
                    if rt.lower() in type_mapping:
                        python_return_types.append(type_mapping[rt.lower()])
                    else:
                        # Use the string as is if not in mapping
                        python_return_types.append(rt)
                
                # Create UDF from code
                # Safely exec the function code to get a callable
                local_namespace = {}
                exec(transformation_function_code, {}, local_namespace)
                
                # Find the function in the namespace
                func = None
                for obj in local_namespace.values():
                    if callable(obj) and not obj.__name__.startswith("__"):
                        func = obj
                        break
                
                if not func:
                    return {
                        "status": "error",
                        "message": "Could not find a valid function in the provided code"
                    }
                
                # If name not provided, use function name
                if not name:
                    name = func.__name__
                
                # Get the udf decorator from hopsworks
                udf_decorator = hopsworks.udf(
                    return_type=python_return_types[0] if len(python_return_types) == 1 else python_return_types,
                    mode=execution_mode
                )
                
                # Apply decorator to get HopsworksUdf object
                decorated_func = udf_decorator(func)
                
                # Create transformation function metadata
                tf_meta = fs.create_transformation_function(
                    transformation_function=decorated_func,
                    version=version
                )
                
                # Save the transformation function to the backend
                tf_meta.save()
                
                # Get transformation function details
                tf_info = {
                    "id": tf_meta.id,
                    "name": name,
                    "version": tf_meta.version,
                    "transformation_type": tf_meta.transformation_type,
                    "output_column_names": tf_meta.output_column_names,
                    "function_source": transformation_function_code,
                    "return_types": return_types,
                    "execution_mode": execution_mode,
                    "status": "created"
                }
                
                return tf_info
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create transformation function: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_transformation_function(
            name: str,
            version: Optional[int] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get information about a transformation function.
            
            Args:
                name: Name of the transformation function
                version: Optional version of the transformation function
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Transformation function information
            """
            if ctx:
                await ctx.info(f"Getting transformation function: {name}{' (v' + str(version) + ')' if version else ''}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get transformation function
                tf = fs.get_transformation_function(name=name, version=version)
                
                # Get function source if available
                function_source = ""
                if hasattr(tf.hopsworks_udf, "func") and callable(tf.hopsworks_udf.func):
                    function_source = inspect.getsource(tf.hopsworks_udf.func)
                
                # Get return types
                return_types = []
                if hasattr(tf.hopsworks_udf, "return_types"):
                    for rt in tf.hopsworks_udf.return_types:
                        # If return type is a class, get its name
                        if isinstance(rt, type):
                            return_types.append(rt.__name__)
                        else:
                            return_types.append(str(rt))
                
                # Get other metadata
                tf_info = {
                    "id": tf.id if hasattr(tf, "id") else None,
                    "name": name,
                    "version": tf.version if hasattr(tf, "version") else None,
                    "transformation_type": tf.transformation_type if hasattr(tf, "transformation_type") else None,
                    "output_column_names": tf.output_column_names if hasattr(tf, "output_column_names") else [],
                    "function_source": function_source,
                    "return_types": return_types,
                    "execution_mode": tf.hopsworks_udf.execution_mode if hasattr(tf.hopsworks_udf, "execution_mode") else "default",
                    "status": "success"
                }
                
                return tf_info
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get transformation function: {str(e)}"
                }
        
        @self.mcp.tool()
        async def delete_transformation_function(
            name: str,
            version: Optional[int] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Delete a transformation function.
            
            Args:
                name: Name of the transformation function to delete
                version: Optional version of the transformation function
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Deleting transformation function: {name}{' (v' + str(version) + ')' if version else ''}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get transformation function
                tf = fs.get_transformation_function(name=name, version=version)
                
                # Delete transformation function
                tf.delete()
                
                return {
                    "name": name,
                    "version": version,
                    "status": "deleted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to delete transformation function: {str(e)}"
                }
        
        @self.mcp.tool()
        async def list_transformation_functions(
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> List[Dict[str, Any]]:
            """List all transformation functions in a project.
            
            Args:
                project_name: Name of the Hopsworks project
                
            Returns:
                list: List of transformation functions
            """
            if ctx:
                await ctx.info(f"Listing transformation functions for project: {project_name or 'default'}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get all transformation functions
                tfs = fs.get_transformation_functions()
                
                # Format the result
                result = []
                for tf in tfs:
                    # Get return types
                    return_types = []
                    if hasattr(tf.hopsworks_udf, "return_types"):
                        for rt in tf.hopsworks_udf.return_types:
                            # If return type is a class, get its name
                            if isinstance(rt, type):
                                return_types.append(rt.__name__)
                            else:
                                return_types.append(str(rt))
                    
                    # Add function info to results
                    result.append({
                        "id": tf.id if hasattr(tf, "id") else None,
                        "name": tf.hopsworks_udf.function_name if hasattr(tf.hopsworks_udf, "function_name") else "unknown",
                        "version": tf.version if hasattr(tf, "version") else None,
                        "transformation_type": tf.transformation_type if hasattr(tf, "transformation_type") else None,
                        "return_types": return_types,
                        "execution_mode": tf.hopsworks_udf.execution_mode if hasattr(tf.hopsworks_udf, "execution_mode") else "default"
                    })
                
                return result
            except Exception as e:
                return [{
                    "status": "error",
                    "message": f"Failed to list transformation functions: {str(e)}"
                }]
        
        @self.mcp.tool()
        async def test_transformation_function(
            function_code: str,
            input_data: Dict[str, Any],
            return_types: Union[str, List[str]],
            execution_mode: Literal["default", "python", "pandas"] = "default",
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Test a transformation function with sample input data.
            
            Args:
                function_code: Python code of the transformation function
                input_data: Sample input data to test the function with
                return_types: Return type(s) of the function (e.g., 'float', ['float', 'int'])
                execution_mode: Mode for function execution ('default', 'python', 'pandas')
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Function test results
            """
            if ctx:
                await ctx.info("Testing transformation function with sample data")
            
            try:
                import pandas as pd
                
                # Convert string return_types to list if needed
                if isinstance(return_types, str):
                    return_types_list = [return_types]
                else:
                    return_types_list = return_types
                
                # Map string types to Python types
                type_mapping = {
                    "int": int,
                    "float": float,
                    "str": str,
                    "bool": bool,
                    "string": str,
                    "integer": int,
                    "boolean": bool
                }
                
                python_return_types = []
                for rt in return_types_list:
                    if rt.lower() in type_mapping:
                        python_return_types.append(type_mapping[rt.lower()])
                    else:
                        # Use the string as is if not in mapping
                        python_return_types.append(rt)
                
                # Create UDF from code
                # Safely exec the function code to get a callable
                local_namespace = {}
                exec(function_code, {}, local_namespace)
                
                # Find the function in the namespace
                func = None
                for obj in local_namespace.values():
                    if callable(obj) and not obj.__name__.startswith("__"):
                        func = obj
                        break
                
                if not func:
                    return {
                        "status": "error",
                        "message": "Could not find a valid function in the provided code"
                    }
                
                # Extract function signature
                signature = inspect.signature(func)
                param_names = list(signature.parameters.keys())
                
                # Create Series from input data
                input_series = {}
                for param in param_names:
                    if param in input_data:
                        input_series[param] = pd.Series([input_data[param]])
                    else:
                        return {
                            "status": "error",
                            "message": f"Missing required input parameter: {param}"
                        }
                
                # Call the function with the input data
                result = func(**input_series)
                
                # Format the result
                if isinstance(result, pd.Series):
                    output = result.to_list()
                elif isinstance(result, pd.DataFrame):
                    output = json.loads(result.to_json(orient='records'))
                else:
                    output = result
                
                return {
                    "function_name": func.__name__,
                    "input": input_data,
                    "output": output,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to test transformation function: {str(e)}"
                }
        
        @self.mcp.tool()
        async def apply_transformation_function(
            name: str,
            input_data: Dict[str, Any],
            version: Optional[int] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Apply a saved transformation function to input data.
            
            Args:
                name: Name of the transformation function
                input_data: Input data to transform
                version: Optional version of the transformation function
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Transformation results
            """
            if ctx:
                await ctx.info(f"Applying transformation function {name} to input data")
            
            try:
                import pandas as pd
                
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get transformation function
                tf = fs.get_transformation_function(name=name, version=version)
                
                # Get the underlying function
                if hasattr(tf.hopsworks_udf, "func") and callable(tf.hopsworks_udf.func):
                    func = tf.hopsworks_udf.func
                else:
                    return {
                        "status": "error",
                        "message": "Could not extract callable function from transformation function"
                    }
                
                # Extract function signature
                signature = inspect.signature(func)
                param_names = list(signature.parameters.keys())
                
                # Create Series from input data
                input_series = {}
                for param in param_names:
                    if param in input_data:
                        input_series[param] = pd.Series([input_data[param]])
                    else:
                        return {
                            "status": "error",
                            "message": f"Missing required input parameter: {param}"
                        }
                
                # Call the function with the input data
                result = func(**input_series)
                
                # Format the result
                if isinstance(result, pd.Series):
                    output = result.to_list()
                elif isinstance(result, pd.DataFrame):
                    output = json.loads(result.to_json(orient='records'))
                else:
                    output = result
                
                return {
                    "transformation_function": name,
                    "version": version,
                    "input": input_data,
                    "output": output,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to apply transformation function: {str(e)}"
                }