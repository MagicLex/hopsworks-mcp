"""Git integration tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks


class GitTools:
    """Tools for working with Git repositories in Hopsworks."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_git_api)
        self.mcp.tool()(self.set_provider)
        self.mcp.tool()(self.get_provider)
        self.mcp.tool()(self.get_providers)
        self.mcp.tool()(self.delete_provider)
        self.mcp.tool()(self.clone_repo)
        self.mcp.tool()(self.get_repo)
        self.mcp.tool()(self.get_repos)
        self.mcp.tool()(self.checkout_branch)
        self.mcp.tool()(self.commit)
        self.mcp.tool()(self.push)
        self.mcp.tool()(self.pull)
        self.mcp.tool()(self.add_remote)
        self.mcp.tool()(self.get_remotes)
        self.mcp.tool()(self.status)
        
    async def get_git_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the Git repository API for the project.
        
        Returns:
            Git API information
        """
        if ctx:
            await ctx.info("Getting Git API for current project")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        return {"connected": True}
    
    async def set_provider(
        self,
        provider: str,
        username: str,
        token: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Configure a Git provider.
        
        Args:
            provider: Name of Git provider (GitHub, GitLab, or BitBucket)
            username: Username for the Git provider service
            token: Token for the Git provider service
            
        Returns:
            Provider configuration status
        """
        if ctx:
            await ctx.info(f"Setting up Git provider: {provider}")
        
        if provider not in ["GitHub", "GitLab", "BitBucket"]:
            return {
                "provider": provider,
                "status": "invalid_provider",
                "message": "Provider must be one of: GitHub, GitLab, BitBucket"
            }
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        git_api.set_provider(provider, username, token)
        
        return {
            "provider": provider,
            "username": username,
            "status": "configured"
        }
    
    async def get_provider(
        self,
        provider: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a configured Git provider.
        
        Args:
            provider: Name of Git provider (GitHub, GitLab, or BitBucket)
            
        Returns:
            Provider information
        """
        if ctx:
            await ctx.info(f"Getting Git provider: {provider}")
        
        if provider not in ["GitHub", "GitLab", "BitBucket"]:
            return {
                "provider": provider,
                "status": "invalid_provider",
                "message": "Provider must be one of: GitHub, GitLab, BitBucket"
            }
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        git_provider = git_api.get_provider(provider)
        
        if not git_provider:
            return {
                "provider": provider,
                "exists": False
            }
            
        return {
            "provider": git_provider.git_provider,
            "username": git_provider.username,
            "exists": True
        }
    
    async def get_providers(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """Get all configured Git providers.
        
        Returns:
            List of provider information
        """
        if ctx:
            await ctx.info("Getting all Git providers")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        providers = git_api.get_providers()
        
        result = []
        for provider in providers:
            result.append({
                "provider": provider.git_provider,
                "username": provider.username
            })
            
        return result
    
    async def delete_provider(
        self,
        provider: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Remove a Git provider configuration.
        
        Args:
            provider: Name of Git provider (GitHub, GitLab, or BitBucket)
            
        Returns:
            Deletion status
        """
        if ctx:
            await ctx.info(f"Deleting Git provider: {provider}")
        
        if provider not in ["GitHub", "GitLab", "BitBucket"]:
            return {
                "provider": provider,
                "status": "invalid_provider",
                "message": "Provider must be one of: GitHub, GitLab, BitBucket"
            }
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        git_provider = git_api.get_provider(provider)
        
        if not git_provider:
            return {
                "provider": provider,
                "status": "not_found"
            }
            
        git_provider.delete()
        
        return {
            "provider": provider,
            "status": "deleted"
        }
    
    async def clone_repo(
        self,
        url: str,
        path: str,
        provider: Optional[str] = None,
        branch: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Clone a Git repository into Hopsworks Filesystem.
        
        Args:
            url: URL to the Git repository
            path: Path in Hopsworks Filesystem to clone the repo to
            provider: The Git provider where the repo is hosted
            branch: Optional branch to clone
            
        Returns:
            Repository information
        """
        if ctx:
            await ctx.info(f"Cloning repository from {url} to {path}")
        
        if provider and provider not in ["GitHub", "GitLab", "BitBucket"]:
            return {
                "url": url,
                "status": "invalid_provider",
                "message": "Provider must be one of: GitHub, GitLab, BitBucket"
            }
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.clone(url, path, provider, branch)
        
        return {
            "id": repo.id,
            "name": repo.name,
            "path": repo.path,
            "provider": repo.provider,
            "current_branch": repo.current_branch,
            "current_commit": repo.current_commit,
            "creator": repo.creator,
            "read_only": repo.read_only,
            "status": "cloned"
        }
    
    async def get_repo(
        self,
        name: str,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a cloned Git repository.
        
        Args:
            name: Name of Git repository
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Repository information
        """
        if ctx:
            await ctx.info(f"Getting Git repository: {name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(name, path)
        
        if not repo:
            return {
                "name": name,
                "exists": False
            }
            
        return {
            "id": repo.id,
            "name": repo.name,
            "path": repo.path,
            "provider": repo.provider,
            "current_branch": repo.current_branch,
            "current_commit": repo.current_commit,
            "creator": repo.creator,
            "read_only": repo.read_only,
            "exists": True
        }
    
    async def get_repos(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """Get all Git repositories in the project.
        
        Returns:
            List of repository information
        """
        if ctx:
            await ctx.info("Getting all Git repositories")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repos = git_api.get_repos()
        
        result = []
        for repo in repos:
            result.append({
                "id": repo.id,
                "name": repo.name,
                "path": repo.path,
                "provider": repo.provider,
                "current_branch": repo.current_branch,
                "current_commit": repo.current_commit,
                "creator": repo.creator,
                "read_only": repo.read_only
            })
            
        return result
    
    async def checkout_branch(
        self,
        repo_name: str,
        branch: str,
        create: bool = False,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Checkout a branch in a Git repository.
        
        Args:
            repo_name: Name of the repository
            branch: Name of the branch
            create: If true, creates a new branch
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Checkout operation status
        """
        if ctx:
            if create:
                await ctx.info(f"Creating and checking out branch {branch} in repository: {repo_name}")
            else:
                await ctx.info(f"Checking out branch {branch} in repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "branch": branch,
                "status": "repository_not_found"
            }
            
        if repo.read_only:
            return {
                "repository": repo_name,
                "branch": branch,
                "status": "read_only_repository"
            }
            
        repo.checkout_branch(branch, create)
        
        return {
            "repository": repo_name,
            "branch": branch,
            "current_branch": repo.current_branch,
            "status": "checked_out"
        }
    
    async def commit(
        self,
        repo_name: str,
        message: str,
        all_changes: bool = True,
        files: Optional[List[str]] = None,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Add changes and commit them in a Git repository.
        
        Args:
            repo_name: Name of the repository
            message: Commit message
            all_changes: Automatically stage modified and deleted files
            files: List of new files to add and commit
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Commit operation status
        """
        if ctx:
            await ctx.info(f"Committing changes in repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "status": "repository_not_found"
            }
            
        if repo.read_only:
            return {
                "repository": repo_name,
                "status": "read_only_repository"
            }
            
        repo.commit(message, all_changes, files)
        
        return {
            "repository": repo_name,
            "commit": repo.current_commit,
            "branch": repo.current_branch,
            "status": "committed"
        }
    
    async def push(
        self,
        repo_name: str,
        branch: str,
        remote: str = "origin",
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Push changes to a remote in a Git repository.
        
        Args:
            repo_name: Name of the repository
            branch: Name of the branch to push
            remote: Name of the remote
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Push operation status
        """
        if ctx:
            await ctx.info(f"Pushing branch {branch} to remote {remote} in repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "status": "repository_not_found"
            }
            
        if repo.read_only:
            return {
                "repository": repo_name,
                "status": "read_only_repository"
            }
            
        repo.push(branch, remote)
        
        return {
            "repository": repo_name,
            "branch": branch,
            "remote": remote,
            "status": "pushed"
        }
    
    async def pull(
        self,
        repo_name: str,
        branch: str,
        remote: str = "origin",
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Pull changes from a remote in a Git repository.
        
        Args:
            repo_name: Name of the repository
            branch: Name of the branch to pull
            remote: Name of the remote
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Pull operation status
        """
        if ctx:
            await ctx.info(f"Pulling branch {branch} from remote {remote} in repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "status": "repository_not_found"
            }
            
        repo.pull(branch, remote)
        
        return {
            "repository": repo_name,
            "branch": branch,
            "remote": remote,
            "current_commit": repo.current_commit,
            "status": "pulled"
        }
    
    async def add_remote(
        self,
        repo_name: str,
        remote_name: str,
        url: str,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Add a remote to a Git repository.
        
        Args:
            repo_name: Name of the repository
            remote_name: Name for the remote
            url: URL of the remote
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Remote addition status
        """
        if ctx:
            await ctx.info(f"Adding remote {remote_name} to repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "status": "repository_not_found"
            }
            
        remote = repo.add_remote(remote_name, url)
        
        return {
            "repository": repo_name,
            "remote": remote.name,
            "url": remote.url,
            "status": "added"
        }
    
    async def get_remotes(
        self,
        repo_name: str,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """Get all remotes for a Git repository.
        
        Args:
            repo_name: Name of the repository
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            List of remote information
        """
        if ctx:
            await ctx.info(f"Getting remotes for repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return []
            
        remotes = repo.get_remotes()
        
        result = []
        for remote in remotes:
            result.append({
                "name": remote.name,
                "url": remote.url
            })
            
        return result
    
    async def status(
        self,
        repo_name: str,
        path: Optional[str] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get the status of a Git repository.
        
        Args:
            repo_name: Name of the repository
            path: Optional path to specify if multiple repos with same name exist
            
        Returns:
            Repository status information
        """
        if ctx:
            await ctx.info(f"Getting status for repository: {repo_name}")
        
        project = hopsworks.get_current_project()
        git_api = project.get_git_api()
        
        repo = git_api.get_repo(repo_name, path)
        
        if not repo:
            return {
                "repository": repo_name,
                "status": "repository_not_found"
            }
            
        status_files = repo.status()
        
        files = []
        for file_status in status_files:
            files.append({
                "file": file_status.file_path,
                "type": file_status.type,
                "status": file_status.status
            })
        
        return {
            "repository": repo_name,
            "branch": repo.current_branch,
            "commit": repo.current_commit,
            "files": files
        }