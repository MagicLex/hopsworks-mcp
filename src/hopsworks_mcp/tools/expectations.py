"""Expectations capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union, Literal
import json
import hopsworks
from fastmcp import Context


class ExpectationTools:
    """Tools for working with Hopsworks data validation expectations."""

    def __init__(self, mcp):
        """Initialize expectation tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_expectation_suite(
            name: str,
            run_validation: bool = True,
            validation_ingestion_policy: Literal["always", "strict"] = "always",
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create an empty expectation suite that can be attached to feature groups.
            
            Args:
                name: Name of the expectation suite
                run_validation: Whether to run validation on insert operations
                validation_ingestion_policy: 'always' (always insert data) or 'strict' (only insert if validation passes)
                
            Returns:
                dict: Expectation suite information
            """
            if ctx:
                await ctx.info(f"Creating empty expectation suite: {name}")
            
            try:
                # Import here to avoid requiring these dependencies for all other tools
                try:
                    import great_expectations as ge
                    ge_available = True
                except ImportError:
                    import hsfs.expectation_suite
                    ge_available = False
                
                if ge_available:
                    # Create using Great Expectations
                    expectation_suite = ge.core.ExpectationSuite(
                        expectation_suite_name=name,
                        expectations=[],
                        meta={"notes": f"Expectation suite created via MCP for Hopsworks."}
                    )
                else:
                    # Create using Hopsworks native API
                    import hsfs.expectation_suite
                    expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                        expectation_suite_name=name,
                        expectations=[],
                        meta={"notes": f"Expectation suite created via MCP for Hopsworks."},
                        run_validation=run_validation,
                        validation_ingestion_policy=validation_ingestion_policy
                    )
                
                # Return basic info - the suite is not yet attached to any feature group
                return {
                    "name": expectation_suite.expectation_suite_name,
                    "run_validation": run_validation,
                    "validation_ingestion_policy": validation_ingestion_policy,
                    "expectations_count": 0,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create expectation suite: {str(e)}"
                }
        
        @self.mcp.tool()
        async def add_column_expectation(
            feature_group_name: str,
            column_name: str,
            expectation_type: str,
            min_value: Optional[Union[int, float]] = None,
            max_value: Optional[Union[int, float]] = None,
            value: Optional[Any] = None,
            value_set: Optional[List[Any]] = None,
            mostly: Optional[float] = None,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Add a column expectation to a feature group's expectation suite.
            
            If the feature group doesn't have an expectation suite, one will be created.
            
            Common expectation types:
            - expect_column_values_to_be_between
            - expect_column_values_to_not_be_null
            - expect_column_values_to_be_unique
            - expect_column_values_to_be_in_set
            - expect_column_values_to_match_regex
            
            Args:
                feature_group_name: Name of the feature group to add the expectation to
                column_name: Name of the column (feature) to validate
                expectation_type: Type of expectation to add
                min_value: Minimum value for range-based expectations
                max_value: Maximum value for range-based expectations
                value: Single value for equality-based expectations
                value_set: Set of allowed values for set-based expectations
                mostly: Fraction of values that must pass (0.0 to 1.0)
                feature_group_version: Version of the feature group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Information about the added expectation
            """
            if ctx:
                await ctx.info(f"Adding {expectation_type} expectation to feature group {feature_group_name} for column {column_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Get or create expectation suite
                try:
                    expectation_suite = fg.get_expectation_suite()
                    is_new = False
                except:
                    # Create new expectation suite if none exists
                    try:
                        import great_expectations as ge
                        expectation_suite = ge.core.ExpectationSuite(
                            expectation_suite_name=f"{feature_group_name}_expectations",
                            expectations=[],
                            meta={"notes": f"Expectation suite for {feature_group_name}"}
                        )
                    except ImportError:
                        import hsfs.expectation_suite
                        expectation_suite = hsfs.expectation_suite.ExpectationSuite(
                            expectation_suite_name=f"{feature_group_name}_expectations",
                            expectations=[],
                            meta={"notes": f"Expectation suite for {feature_group_name}"}
                        )
                    is_new = True
                
                # Create kwargs for the expectation
                kwargs = {"column": column_name}
                
                if min_value is not None and max_value is not None:
                    kwargs["min_value"] = min_value
                    kwargs["max_value"] = max_value
                
                if value is not None:
                    kwargs["value"] = value
                
                if value_set is not None:
                    kwargs["value_set"] = value_set
                
                if mostly is not None:
                    kwargs["mostly"] = mostly
                
                # Create the expectation
                try:
                    import great_expectations as ge
                    expectation = ge.core.ExpectationConfiguration(
                        expectation_type=expectation_type,
                        kwargs=kwargs
                    )
                except ImportError:
                    import hsfs.ge_expectation
                    expectation = hsfs.ge_expectation.GeExpectation(
                        expectation_type=expectation_type,
                        kwargs=kwargs
                    )
                
                # Add the expectation to the suite
                added = expectation_suite.add_expectation(expectation)
                
                # If this is a new suite, attach it to the feature group
                if is_new:
                    fg.save_expectation_suite(expectation_suite)
                
                # Return information about the added expectation
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "column": column_name,
                    "expectation_type": expectation_type,
                    "expectation_id": added.meta.get("expectationId") if hasattr(added, "meta") else None,
                    "parameters": kwargs,
                    "status": "added"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to add expectation: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_feature_group_expectations(
            feature_group_name: str,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get all expectations for a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                feature_group_version: Version of the feature group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: List of expectations in the feature group's expectation suite
            """
            if ctx:
                await ctx.info(f"Getting expectations for feature group: {feature_group_name} (v{feature_group_version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                try:
                    expectation_suite = fg.get_expectation_suite()
                    
                    # Extract expectations in a serializable format
                    expectations = []
                    for exp in expectation_suite.expectations:
                        exp_data = {
                            "id": exp.meta.get("expectationId") if hasattr(exp, "meta") else None,
                            "type": exp.expectation_type if hasattr(exp, "expectation_type") else None,
                            "column": exp.kwargs.get("column") if hasattr(exp, "kwargs") else None,
                            "parameters": exp.kwargs if hasattr(exp, "kwargs") else {}
                        }
                        expectations.append(exp_data)
                    
                    return {
                        "feature_group": feature_group_name,
                        "feature_group_version": feature_group_version,
                        "suite_name": expectation_suite.expectation_suite_name if hasattr(expectation_suite, "expectation_suite_name") else None,
                        "run_validation": expectation_suite.run_validation if hasattr(expectation_suite, "run_validation") else True,
                        "validation_ingestion_policy": expectation_suite.validation_ingestion_policy if hasattr(expectation_suite, "validation_ingestion_policy") else "always",
                        "expectations": expectations,
                        "status": "success"
                    }
                except Exception as e:
                    return {
                        "feature_group": feature_group_name,
                        "feature_group_version": feature_group_version,
                        "status": "no_expectations",
                        "message": f"No expectation suite found for feature group: {str(e)}"
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get expectations: {str(e)}"
                }
        
        @self.mcp.tool()
        async def remove_expectation(
            feature_group_name: str,
            expectation_id: int,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Remove a specific expectation from a feature group's expectation suite.
            
            Args:
                feature_group_name: Name of the feature group
                expectation_id: ID of the expectation to remove
                feature_group_version: Version of the feature group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Removing expectation {expectation_id} from feature group: {feature_group_name}")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                expectation_suite = fg.get_expectation_suite()
                expectation_suite.remove_expectation(expectation_id=expectation_id)
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "expectation_id": expectation_id,
                    "status": "removed"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to remove expectation: {str(e)}"
                }
                
        @self.mcp.tool()
        async def validate_data(
            feature_group_name: str,
            data: str,  # JSON string
            feature_group_version: int = 1,
            save_report: bool = False,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Validate a dataset against a feature group's expectation suite.
            
            Args:
                feature_group_name: Name of the feature group
                data: JSON string representation of the data to validate
                feature_group_version: Version of the feature group
                save_report: Whether to save the validation report to Hopsworks
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Validation results
            """
            if ctx:
                await ctx.info(f"Validating data against feature group: {feature_group_name} expectations")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Convert JSON to pandas dataframe
                import pandas as pd
                try:
                    df = pd.read_json(data, orient='records')
                except:
                    return {
                        "status": "error",
                        "message": "Invalid JSON data format. Expected a list of records."
                    }
                
                # Validate data against the feature group's expectations
                validation_report = fg.validate(df, save_report=save_report)
                
                # Process results
                if hasattr(validation_report, 'success'):
                    success = validation_report.success
                elif isinstance(validation_report, dict) and 'success' in validation_report:
                    success = validation_report['success']
                else:
                    success = False
                
                # Extract and format expectations results
                expectations_results = []
                if hasattr(validation_report, 'results'):
                    results = validation_report.results
                elif isinstance(validation_report, dict) and 'results' in validation_report:
                    results = validation_report['results']
                else:
                    results = []
                
                for result in results:
                    if isinstance(result, dict):
                        expectation_data = {
                            "type": result.get('expectation_config', {}).get('expectation_type'),
                            "column": result.get('expectation_config', {}).get('kwargs', {}).get('column'),
                            "success": result.get('success', False),
                            "observed_value": result.get('result', {}).get('observed_value')
                        }
                    else:
                        expectation_data = {
                            "type": result.expectation_config.expectation_type if hasattr(result, 'expectation_config') else None,
                            "column": result.expectation_config.kwargs.get('column') if hasattr(result, 'expectation_config') else None,
                            "success": result.success if hasattr(result, 'success') else False,
                            "observed_value": result.result.get('observed_value') if hasattr(result, 'result') else None
                        }
                    
                    expectations_results.append(expectation_data)
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "rows_validated": len(df),
                    "validation_success": success,
                    "expectations_results": expectations_results,
                    "save_report": save_report,
                    "status": "validated"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to validate data: {str(e)}"
                }
        
        @self.mcp.tool()
        async def get_validation_history(
            feature_group_name: str,
            feature_group_version: int = 1,
            limit: int = 5,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get the validation history for a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                feature_group_version: Version of the feature group
                limit: Maximum number of validation reports to return
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: List of validation reports
            """
            if ctx:
                await ctx.info(f"Getting validation history for feature group: {feature_group_name} (v{feature_group_version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Try to get regular feature group first
                try:
                    fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                except:
                    # If that fails, try external feature group
                    fg = fs.get_external_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Get validation reports
                validation_reports = fg.get_all_validation_reports()
                
                # Extract key information in a serializable format
                reports = []
                for report in validation_reports[:limit]:
                    report_info = {
                        "id": report.id if hasattr(report, 'id') else None,
                        "time": str(report.run_time) if hasattr(report, 'run_time') else None,
                        "success": report.success if hasattr(report, 'success') else False,
                        "expectations_passed": len([r for r in report.results if r.success]) if hasattr(report, 'results') else 0,
                        "expectations_failed": len([r for r in report.results if not r.success]) if hasattr(report, 'results') else 0,
                        "total_expectations": len(report.results) if hasattr(report, 'results') else 0,
                    }
                    reports.append(report_info)
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "reports": reports,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get validation history: {str(e)}"
                }