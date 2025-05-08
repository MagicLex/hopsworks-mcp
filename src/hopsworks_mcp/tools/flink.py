"""Flink cluster tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks
import os


class FlinkTools:
    """Tools for working with Hopsworks Flink clusters."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_flink_cluster_api)
        self.mcp.tool()(self.setup_cluster)
        self.mcp.tool()(self.get_cluster)
        self.mcp.tool()(self.start_cluster)
        self.mcp.tool()(self.stop_cluster)
        self.mcp.tool()(self.upload_jar)
        self.mcp.tool()(self.get_jars)
        self.mcp.tool()(self.submit_job)
        self.mcp.tool()(self.get_flink_jobs)  # Renamed from get_jobs
        self.mcp.tool()(self.get_flink_job)   # Renamed from get_job
        self.mcp.tool()(self.job_state)
        self.mcp.tool()(self.stop_job)
        
    async def get_flink_cluster_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the Flink cluster API for the project.
        
        Returns:
            Flink cluster API information
        """
        if ctx:
            await ctx.info("Getting Flink cluster API for current project")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        
        return {"connected": True}
    
    async def setup_cluster(
        self,
        name: str,
        config: Optional[Dict[str, Any]] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create a new Flink cluster or update an existing one.
        
        Args:
            name: Name of the cluster
            config: Configuration for the cluster
            
        Returns:
            Cluster information
        """
        if ctx:
            await ctx.info(f"Setting up Flink cluster: {name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        
        # If no config provided, get default configuration
        if not config:
            config = flink_api.get_configuration()
            config['appName'] = name
        
        cluster = flink_api.setup_cluster(name=name, config=config)
        
        return {
            "id": cluster.id,
            "name": cluster.name,
            "state": cluster.state,
            "creation_time": str(cluster.creation_time),
            "creator": cluster.creator
        }
    
    async def get_cluster(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a Flink cluster by name.
        
        Args:
            name: Name of the cluster
            
        Returns:
            Cluster information
        """
        if ctx:
            await ctx.info(f"Getting Flink cluster: {name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=name)
        
        if not cluster:
            return {
                "name": name,
                "exists": False
            }
            
        return {
            "id": cluster.id,
            "name": cluster.name,
            "state": cluster.state,
            "creation_time": str(cluster.creation_time),
            "creator": cluster.creator,
            "exists": True
        }
    
    async def start_cluster(
        self,
        name: str,
        await_time: int = 1800,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Start a Flink cluster.
        
        Args:
            name: Name of the cluster
            await_time: Time to wait for the cluster to start (in seconds)
            
        Returns:
            Start operation status
        """
        if ctx:
            await ctx.info(f"Starting Flink cluster: {name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=name)
        
        if not cluster:
            return {
                "name": name,
                "status": "not_found"
            }
            
        cluster.start(await_time=await_time)
        
        return {
            "id": cluster.id,
            "name": cluster.name,
            "state": cluster.state,
            "status": "started"
        }
    
    async def stop_cluster(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Stop a Flink cluster.
        
        Args:
            name: Name of the cluster
            
        Returns:
            Stop operation status
        """
        if ctx:
            await ctx.info(f"Stopping Flink cluster: {name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=name)
        
        if not cluster:
            return {
                "name": name,
                "status": "not_found"
            }
            
        cluster.stop()
        
        return {
            "id": cluster.id,
            "name": cluster.name,
            "status": "stopped"
        }
    
    async def upload_jar(
        self,
        cluster_name: str,
        jar_file_path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Upload a JAR file to a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            jar_file_path: Path to the JAR file
            
        Returns:
            Upload status
        """
        if ctx:
            await ctx.info(f"Uploading JAR file {jar_file_path} to Flink cluster: {cluster_name}")
        
        if not os.path.exists(jar_file_path):
            return {
                "cluster": cluster_name,
                "jar_file": jar_file_path,
                "status": "file_not_found"
            }
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return {
                "cluster": cluster_name,
                "status": "cluster_not_found"
            }
            
        cluster.upload_jar(jar_file_path)
        
        return {
            "cluster": cluster_name,
            "jar_file": jar_file_path,
            "status": "uploaded"
        }
    
    async def get_jars(
        self,
        cluster_name: str,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """Get all JAR files uploaded to a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of JAR files
        """
        if ctx:
            await ctx.info(f"Getting JAR files from Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return []
            
        jars = cluster.get_jars()
        
        return jars
    
    async def submit_job(
        self,
        cluster_name: str,
        jar_id: str,
        main_class: str,
        job_arguments: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Submit a job to a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            jar_id: ID of the JAR file
            main_class: Path to the main class
            job_arguments: Job arguments
            
        Returns:
            Job submission status
        """
        if ctx:
            await ctx.info(f"Submitting job to Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return {
                "cluster": cluster_name,
                "status": "cluster_not_found"
            }
            
        job_id = cluster.submit_job(
            jar_id=jar_id,
            main_class=main_class,
            job_arguments=job_arguments
        )
        
        return {
            "cluster": cluster_name,
            "job_id": job_id,
            "status": "submitted"
        }
    
    async def get_flink_jobs(
        self,
        cluster_name: str,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """Get all jobs in a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            
        Returns:
            List of jobs
        """
        if ctx:
            await ctx.info(f"Getting jobs from Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return []
            
        jobs = cluster.get_jobs()
        
        return jobs
    
    async def get_flink_job(
        self,
        cluster_name: str,
        job_id: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get details of a specific job in a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            job_id: ID of the job
            
        Returns:
            Job details
        """
        if ctx:
            await ctx.info(f"Getting job {job_id} from Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return {
                "cluster": cluster_name,
                "job_id": job_id,
                "status": "cluster_not_found"
            }
            
        job = cluster.get_job(job_id)
        
        return job
    
    async def job_state(
        self,
        cluster_name: str,
        job_id: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get the state of a job in a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            job_id: ID of the job
            
        Returns:
            Job state
        """
        if ctx:
            await ctx.info(f"Getting state of job {job_id} in Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return {
                "cluster": cluster_name,
                "job_id": job_id,
                "status": "cluster_not_found"
            }
            
        state = cluster.job_state(job_id)
        
        return {
            "cluster": cluster_name,
            "job_id": job_id,
            "state": state
        }
    
    async def stop_job(
        self,
        cluster_name: str,
        job_id: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Stop a job in a Flink cluster.
        
        Args:
            cluster_name: Name of the cluster
            job_id: ID of the job
            
        Returns:
            Stop operation status
        """
        if ctx:
            await ctx.info(f"Stopping job {job_id} in Flink cluster: {cluster_name}")
        
        project = hopsworks.get_current_project()
        flink_api = project.get_flink_cluster_api()
        cluster = flink_api.get_cluster(name=cluster_name)
        
        if not cluster:
            return {
                "cluster": cluster_name,
                "job_id": job_id,
                "status": "cluster_not_found"
            }
            
        cluster.stop_job(job_id)
        
        return {
            "cluster": cluster_name,
            "job_id": job_id,
            "status": "stopped"
        }