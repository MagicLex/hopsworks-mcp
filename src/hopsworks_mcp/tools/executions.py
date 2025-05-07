"""Job execution tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks


class ExecutionTools:
    """Tools for working with Hopsworks job executions."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.run_job)
        self.mcp.tool()(self.get_executions)
        self.mcp.tool()(self.get_execution_status)
        self.mcp.tool()(self.stop_execution)
        self.mcp.tool()(self.download_execution_logs)
        
    async def run_job(
        self,
        job_name: str,
        job_type: str = "FEATURE_GROUP_INSERT",
        args: Optional[str] = None,
        await_termination: bool = True,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Run a job in Hopsworks.
        
        Args:
            job_name: Name of the job to run
            job_type: Type of the job
            args: Optional runtime arguments for the job
            await_termination: Whether to wait for the job to complete
            
        Returns:
            Execution information
        """
        if ctx:
            await ctx.info(f"Running job: {job_name}")
        
        # Note: In a real implementation, we would need to first get the job by name
        # Hopsworks API doesn't directly expose a way to get a job by name,
        # so typically we'd need to get the job from its context (like a feature group)
        # 
        # This demonstrates the principle, but would need to be modified based on
        # how the job was created:
        
        project = hopsworks.get_current_project()
        
        # Example for a feature group job:
        # fs = project.get_feature_store()
        # fg = fs.get_feature_group(name=feature_group_name)
        # job, _ = fg.insert(df, write_options={"start_offline_materialization": False})
        # 
        # or for a training job:
        # model_registry = project.get_model_registry()
        # job = model_registry.get_training_job(name=job_name)
        
        # Since we don't have full context here, we'll note that this is a placeholder
        # In practice, this function would be specialized based on job type
        # or would need additional parameters to identify the job
        
        # Placeholder for job execution
        job = None
        
        if not job:
            return {
                "job_name": job_name,
                "status": "not_found",
                "message": "Job not found or insufficient context to locate job"
            }
            
        execution = job.run(args=args, await_termination=await_termination)
        
        result = {
            "execution_id": execution.id,
            "job_name": execution.job_name,
            "state": execution.state,
            "success": execution.success,
            "submission_time": str(execution.submission_time),
            "duration": execution.duration,
            "app_id": execution.app_id
        }
        
        if execution.final_status:
            result["final_status"] = execution.final_status
            
        return result
    
    async def get_executions(
        self,
        job_name: str,
        job_type: str = "FEATURE_GROUP_INSERT",
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """Get all executions for a job.
        
        Args:
            job_name: Name of the job
            job_type: Type of the job
            
        Returns:
            List of executions
        """
        if ctx:
            await ctx.info(f"Getting executions for job: {job_name}")
        
        # Same note as in run_job - we need job context
        project = hopsworks.get_current_project()
        
        # Placeholder - in real implementation we'd get the job first
        job = None
        
        if not job:
            return []
            
        executions = job.get_executions()
        
        result = []
        for execution in executions:
            exec_info = {
                "execution_id": execution.id,
                "job_name": execution.job_name,
                "state": execution.state,
                "success": execution.success,
                "submission_time": str(execution.submission_time),
                "duration": execution.duration,
                "app_id": execution.app_id
            }
            
            if execution.final_status:
                exec_info["final_status"] = execution.final_status
                
            result.append(exec_info)
            
        return result
    
    async def get_execution_status(
        self,
        execution_id: int,
        await_termination: bool = False,
        timeout: Optional[float] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get status of a specific execution.
        
        Args:
            execution_id: ID of the execution
            await_termination: Whether to wait for the execution to complete
            timeout: Maximum waiting time in seconds
            
        Returns:
            Execution status information
        """
        if ctx:
            await ctx.info(f"Getting status for execution: {execution_id}")
        
        # Note: Getting an execution by ID directly is not straightforward in Hopsworks API
        # We would typically need to get the job first, then find the execution
        # This is a placeholder for demonstration
        
        project = hopsworks.get_current_project()
        
        # Placeholder - in a real implementation, we'd need to find the execution
        execution = None
        
        if not execution:
            return {
                "execution_id": execution_id,
                "status": "not_found",
                "message": "Execution not found or insufficient context to locate execution"
            }
            
        if await_termination:
            execution.await_termination(timeout=timeout)
            
        return {
            "execution_id": execution.id,
            "job_name": execution.job_name,
            "state": execution.state,
            "success": execution.success,
            "submission_time": str(execution.submission_time),
            "duration": execution.duration,
            "app_id": execution.app_id,
            "final_status": execution.final_status if execution.final_status else None
        }
    
    async def stop_execution(
        self,
        execution_id: int,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Stop a running execution.
        
        Args:
            execution_id: ID of the execution to stop
            
        Returns:
            Stop operation status
        """
        if ctx:
            await ctx.info(f"Stopping execution: {execution_id}")
            await ctx.info("WARNING: This is a potentially dangerous operation")
        
        # Note: Same limitation as get_execution_status - finding the execution requires context
        project = hopsworks.get_current_project()
        
        # Placeholder - in a real implementation, we'd need to find the execution
        execution = None
        
        if not execution:
            return {
                "execution_id": execution_id,
                "status": "not_found",
                "message": "Execution not found or insufficient context to locate execution"
            }
            
        execution.stop()
        
        return {
            "execution_id": execution_id,
            "status": "stopped"
        }
    
    async def download_execution_logs(
        self,
        execution_id: int,
        download_path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Download stdout and stderr logs for an execution.
        
        Args:
            execution_id: ID of the execution
            download_path: Path where to download the logs
            
        Returns:
            Paths to downloaded logs
        """
        if ctx:
            await ctx.info(f"Downloading logs for execution: {execution_id}")
        
        # Note: Same limitation as other methods - finding the execution requires context
        project = hopsworks.get_current_project()
        
        # Placeholder - in a real implementation, we'd need to find the execution
        execution = None
        
        if not execution:
            return {
                "execution_id": execution_id,
                "status": "not_found",
                "message": "Execution not found or insufficient context to locate execution"
            }
            
        stdout_path, stderr_path = execution.download_logs(path=download_path)
        
        return {
            "execution_id": execution_id,
            "stdout_path": stdout_path,
            "stderr_path": stderr_path,
            "status": "downloaded"
        }