"""Job management tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks
from datetime import datetime, timezone


class JobTools:
    """Tools for working with Hopsworks jobs."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_job_api)
        self.mcp.tool()(self.get_configuration)
        self.mcp.tool()(self.create_job)
        self.mcp.tool()(self.get_job)
        self.mcp.tool()(self.get_jobs)
        self.mcp.tool()(self.delete_job)
        self.mcp.tool()(self.update_job)
        self.mcp.tool()(self.schedule_job)
        self.mcp.tool()(self.unschedule_job)
        self.mcp.tool()(self.pause_schedule)
        self.mcp.tool()(self.resume_schedule)
        self.mcp.tool()(self.get_job_state)
        
    async def get_job_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the job API for the project.
        
        Returns:
            Job API information
        """
        if ctx:
            await ctx.info("Getting job API for current project")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        return {"connected": True}
    
    async def get_configuration(
        self,
        job_type: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get configuration for a specific job type.
        
        Args:
            job_type: Type of the job (SPARK, PYSPARK, PYTHON, DOCKER, FLINK)
            
        Returns:
            Job configuration template
        """
        if ctx:
            await ctx.info(f"Getting configuration for job type: {job_type}")
        
        valid_types = ["SPARK", "PYSPARK", "PYTHON", "DOCKER", "FLINK"]
        if job_type not in valid_types:
            return {
                "job_type": job_type,
                "status": "invalid_type",
                "message": f"Job type must be one of: {', '.join(valid_types)}"
            }
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        config = job_api.get_configuration(job_type)
        
        return config
    
    async def create_job(
        self,
        name: str,
        config: Dict[str, Any],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create a new job or update an existing one.
        
        Args:
            name: Name of the job
            config: Configuration of the job
            
        Returns:
            Job information
        """
        if ctx:
            await ctx.info(f"Creating job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.create_job(name, config)
        
        return {
            "id": job.id,
            "name": job.name,
            "job_type": job.job_type,
            "creator": job.creator,
            "creation_time": str(job.creation_time),
            "status": "created"
        }
    
    async def get_job(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a job by name.
        
        Args:
            name: Name of the job
            
        Returns:
            Job information
        """
        if ctx:
            await ctx.info(f"Getting job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "exists": False
            }
            
        job_info = {
            "id": job.id,
            "name": job.name,
            "job_type": job.job_type,
            "creator": job.creator,
            "creation_time": str(job.creation_time),
            "config": job.config,
            "exists": True
        }
        
        # Add schedule information if available
        if job.job_schedule:
            job_info["schedule"] = {
                "cron_expression": job.job_schedule.cron_expression,
                "start_time": str(job.job_schedule.start_time),
                "end_time": str(job.job_schedule.end_time) if job.job_schedule.end_time else None,
                "next_execution": str(job.job_schedule.next_execution_date_time) if job.job_schedule.next_execution_date_time else None
            }
            
        return job_info
    
    async def get_jobs(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """Get all jobs in the project.
        
        Returns:
            List of job information
        """
        if ctx:
            await ctx.info("Getting all jobs")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        jobs = job_api.get_jobs()
        
        result = []
        for job in jobs:
            job_info = {
                "id": job.id,
                "name": job.name,
                "job_type": job.job_type,
                "creator": job.creator,
                "creation_time": str(job.creation_time)
            }
            
            # Add schedule information if available
            if job.job_schedule:
                job_info["schedule"] = {
                    "cron_expression": job.job_schedule.cron_expression,
                    "start_time": str(job.job_schedule.start_time),
                    "end_time": str(job.job_schedule.end_time) if job.job_schedule.end_time else None,
                    "next_execution": str(job.job_schedule.next_execution_date_time) if job.job_schedule.next_execution_date_time else None
                }
                
            result.append(job_info)
            
        return result
    
    async def delete_job(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Delete a job.
        
        Args:
            name: Name of the job
            
        Returns:
            Deletion status
        """
        if ctx:
            await ctx.info(f"Deleting job: {name}")
            await ctx.info("WARNING: This will delete the job and all its executions")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        job.delete()
        
        return {
            "name": name,
            "status": "deleted"
        }
    
    async def update_job(
        self,
        name: str,
        config: Dict[str, Any],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Update an existing job's configuration.
        
        Args:
            name: Name of the job
            config: New configuration for the job
            
        Returns:
            Update status
        """
        if ctx:
            await ctx.info(f"Updating job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        # Update configuration
        job.config = config
        
        # Save changes
        job.save()
        
        return {
            "id": job.id,
            "name": job.name,
            "status": "updated"
        }
    
    async def schedule_job(
        self,
        name: str,
        cron_expression: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Schedule the execution of a job.
        
        Args:
            name: Name of the job
            cron_expression: Quartz cron expression
            start_time: Optional start time (ISO format)
            end_time: Optional end time (ISO format)
            
        Returns:
            Schedule information
        """
        if ctx:
            await ctx.info(f"Scheduling job: {name} with cron expression: {cron_expression}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        # Convert string times to datetime if provided
        start_datetime = None
        if start_time:
            start_datetime = datetime.fromisoformat(start_time).replace(tzinfo=timezone.utc)
            
        end_datetime = None
        if end_time:
            end_datetime = datetime.fromisoformat(end_time).replace(tzinfo=timezone.utc)
            
        # Schedule the job
        schedule = job.schedule(
            cron_expression=cron_expression,
            start_time=start_datetime,
            end_time=end_datetime
        )
        
        return {
            "name": job.name,
            "cron_expression": schedule.cron_expression,
            "start_time": str(schedule.start_time),
            "end_time": str(schedule.end_time) if schedule.end_time else None,
            "next_execution": str(schedule.next_execution_date_time) if schedule.next_execution_date_time else None,
            "status": "scheduled"
        }
    
    async def unschedule_job(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Unschedule the execution of a job.
        
        Args:
            name: Name of the job
            
        Returns:
            Unschedule status
        """
        if ctx:
            await ctx.info(f"Unscheduling job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        if not job.job_schedule:
            return {
                "name": name,
                "status": "not_scheduled"
            }
            
        job.unschedule()
        
        return {
            "name": name,
            "status": "unscheduled"
        }
    
    async def pause_schedule(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Pause the schedule of a job.
        
        Args:
            name: Name of the job
            
        Returns:
            Pause status
        """
        if ctx:
            await ctx.info(f"Pausing schedule for job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        if not job.job_schedule:
            return {
                "name": name,
                "status": "not_scheduled"
            }
            
        job.pause_schedule()
        
        return {
            "name": name,
            "status": "paused"
        }
    
    async def resume_schedule(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Resume the schedule of a job.
        
        Args:
            name: Name of the job
            
        Returns:
            Resume status
        """
        if ctx:
            await ctx.info(f"Resuming schedule for job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        if not job.job_schedule:
            return {
                "name": name,
                "status": "not_scheduled"
            }
            
        job.resume_schedule()
        
        return {
            "name": name,
            "status": "resumed"
        }
    
    async def get_job_state(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get the state of a job.
        
        Args:
            name: Name of the job
            
        Returns:
            Job state information
        """
        if ctx:
            await ctx.info(f"Getting state for job: {name}")
        
        project = hopsworks.get_current_project()
        job_api = project.get_job_api()
        
        job = job_api.get_job(name)
        
        if not job:
            return {
                "name": name,
                "status": "not_found"
            }
            
        state = job.get_state()
        final_state = job.get_final_state()
        
        return {
            "name": name,
            "state": state,
            "final_state": final_state
        }