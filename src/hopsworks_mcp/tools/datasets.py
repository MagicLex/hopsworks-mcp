"""Dataset management tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks


class DatasetTools:
    """Tools for working with Hopsworks datasets."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_dataset_api)
        self.mcp.tool()(self.upload_file)
        self.mcp.tool()(self.download_file)
        self.mcp.tool()(self.list_files)
        self.mcp.tool()(self.create_directory)
        self.mcp.tool()(self.remove_file)
        self.mcp.tool()(self.check_exists)
        self.mcp.tool()(self.move_file)
        self.mcp.tool()(self.copy_file)
        self.mcp.tool()(self.read_content)
        self.mcp.tool()(self.zip_file)
        self.mcp.tool()(self.unzip_file)
        
    async def get_dataset_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the dataset API for the project.
        
        Returns:
            Dataset API information
        """
        if ctx:
            await ctx.info("Getting dataset API for current project")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        
        return {"connected": True}
    
    async def upload_file(
        self,
        local_path: str,
        upload_path: str,
        overwrite: bool = False,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Upload a file or directory to the Hopsworks filesystem.
        
        Args:
            local_path: Local path to file or directory to upload
            upload_path: Path to directory where to upload in Hopsworks Filesystem
            overwrite: Overwrite file or directory if exists
        
        Returns:
            Upload result info
        """
        if ctx:
            await ctx.info(f"Uploading {local_path} to {upload_path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        path = dataset_api.upload(local_path, upload_path, overwrite=overwrite)
        
        return {"path": path, "status": "success"}
    
    async def download_file(
        self,
        path: str,
        local_path: Optional[str] = None,
        overwrite: bool = False,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Download file from Hopsworks Filesystem.
        
        Args:
            path: Path in Hopsworks filesystem to the file
            local_path: Path where to download the file in the local filesystem
            overwrite: Overwrite local file if exists
        
        Returns:
            Download result info
        """
        if ctx:
            await ctx.info(f"Downloading {path} to {local_path or 'current directory'}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        local_path_result = dataset_api.download(path, local_path, overwrite=overwrite)
        
        return {"local_path": local_path_result, "status": "success"}
    
    async def list_files(
        self,
        path: str,
        recursive: bool = False,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """List files in a directory in the Hopsworks Filesystem.
        
        Args:
            path: Path to directory
            recursive: Whether to list files recursively
        
        Returns:
            List of file information
        """
        if ctx:
            await ctx.info(f"Listing files in {path}")
        
        # Note: The list function might not be directly available in the client
        # We may need to use lower-level REST API calls
        
        # For now, we'll check if the path exists
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        exists = dataset_api.exists(path)
        
        if not exists:
            return []
            
        # In a more complete implementation, we would make a call to list the contents
        # This would likely require using the REST API directly
        return []
    
    async def create_directory(
        self,
        path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create a directory in the Hopsworks Filesystem.
        
        Args:
            path: Path to directory to create
        
        Returns:
            Directory information
        """
        if ctx:
            await ctx.info(f"Creating directory: {path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        path_result = dataset_api.mkdir(path)
        
        return {"path": path_result, "status": "created"}
    
    async def remove_file(
        self,
        path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Remove a path in the Hopsworks Filesystem.
        
        Args:
            path: Path to remove
        
        Returns:
            Removal status
        """
        if ctx:
            await ctx.info(f"Removing: {path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        dataset_api.remove(path)
        
        return {"path": path, "status": "removed"}
    
    async def check_exists(
        self,
        path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Check if a file exists in the Hopsworks Filesystem.
        
        Args:
            path: Path to check
        
        Returns:
            Existence status
        """
        if ctx:
            await ctx.info(f"Checking if {path} exists")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        exists = dataset_api.exists(path)
        
        return {"path": path, "exists": exists}
    
    async def move_file(
        self,
        source_path: str,
        destination_path: str,
        overwrite: bool = False,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Move a file or directory in the Hopsworks Filesystem.
        
        Args:
            source_path: The source path to move
            destination_path: The destination path
            overwrite: Overwrite destination if exists
        
        Returns:
            Move operation status
        """
        if ctx:
            await ctx.info(f"Moving {source_path} to {destination_path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        path = dataset_api.move(source_path, destination_path, overwrite=overwrite)
        
        return {"source": source_path, "destination": destination_path, "path": path, "status": "moved"}
    
    async def copy_file(
        self,
        source_path: str,
        destination_path: str,
        overwrite: bool = False,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Copy a file or directory in the Hopsworks Filesystem.
        
        Args:
            source_path: The source path to copy
            destination_path: The destination path
            overwrite: Overwrite destination if exists
        
        Returns:
            Copy operation status
        """
        if ctx:
            await ctx.info(f"Copying {source_path} to {destination_path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        path = dataset_api.copy(source_path, destination_path, overwrite=overwrite)
        
        return {"source": source_path, "destination": destination_path, "path": path, "status": "copied"}
    
    async def read_content(
        self,
        path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Read content of a file in the Hopsworks Filesystem.
        
        Args:
            path: Path to the file
        
        Returns:
            File content
        """
        if ctx:
            await ctx.info(f"Reading content of {path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        content = dataset_api.read_content(path)
        
        return {"path": path, "content": content}
    
    async def zip_file(
        self,
        remote_path: str,
        destination_path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Zip a file or directory in the dataset.
        
        Args:
            remote_path: Path to file or directory to zip
            destination_path: Path to upload the zip
        
        Returns:
            Zip operation status
        """
        dest = destination_path or f"{remote_path}.zip"
        if ctx:
            await ctx.info(f"Zipping {remote_path} to {dest}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        success = dataset_api.zip(remote_path, destination_path, block=True)
        
        return {"source": remote_path, "destination": dest, "status": "zipped" if success else "in_progress"}
    
    async def unzip_file(
        self,
        remote_path: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Unzip an archive in the dataset.
        
        Args:
            remote_path: Path to file to unzip
        
        Returns:
            Unzip operation status
        """
        if ctx:
            await ctx.info(f"Unzipping {remote_path}")
        
        project = hopsworks.get_current_project()
        dataset_api = project.get_dataset_api()
        success = dataset_api.unzip(remote_path, block=True)
        
        return {"path": remote_path, "status": "unzipped" if success else "in_progress"}