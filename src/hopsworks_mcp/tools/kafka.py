"""Kafka tools for Hopsworks."""

from fastmcp import Context
from typing import Dict, Any, Optional, List
import hopsworks


class KafkaTools:
    """Tools for working with Kafka in Hopsworks."""

    def __init__(self, mcp):
        self.mcp = mcp
        
        # Register tools
        self.mcp.tool()(self.get_kafka_api)
        self.mcp.tool()(self.get_default_config)
        self.mcp.tool()(self.create_schema)
        self.mcp.tool()(self.get_schema)
        self.mcp.tool()(self.get_schemas)
        self.mcp.tool()(self.get_subjects)
        self.mcp.tool()(self.delete_schema)
        self.mcp.tool()(self.create_topic)
        self.mcp.tool()(self.get_topic)
        self.mcp.tool()(self.get_topics)
        self.mcp.tool()(self.delete_topic)
        
    async def get_kafka_api(self, ctx: Context = None) -> Dict[str, Any]:
        """Get the Kafka API for the project.
        
        Returns:
            Kafka API information
        """
        if ctx:
            await ctx.info("Getting Kafka API for current project")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        return {"connected": True}
    
    async def get_default_config(
        self,
        internal_kafka: Optional[bool] = None,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get the configuration for a Kafka Producer or Consumer.
        
        Args:
            internal_kafka: Whether to use the internal Kafka broker
            
        Returns:
            Kafka configuration dictionary
        """
        if ctx:
            await ctx.info("Getting default Kafka configuration")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        config = kafka_api.get_default_config(internal_kafka=internal_kafka)
        
        return config
    
    async def create_schema(
        self,
        subject: str,
        schema: Dict[str, Any],
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create a new Kafka schema.
        
        Args:
            subject: Subject name of the schema
            schema: Avro schema definition
            
        Returns:
            Schema information
        """
        if ctx:
            await ctx.info(f"Creating Kafka schema: {subject}")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        kafka_schema = kafka_api.create_schema(subject, schema)
        
        return {
            "id": kafka_schema.id,
            "subject": kafka_schema.subject,
            "version": kafka_schema.version,
            "status": "created"
        }
    
    async def get_schema(
        self,
        subject: str,
        version: int,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a Kafka schema by subject and version.
        
        Args:
            subject: Subject name
            version: Version number
            
        Returns:
            Schema information
        """
        if ctx:
            await ctx.info(f"Getting Kafka schema: {subject} (version {version})")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        schema = kafka_api.get_schema(subject, version)
        
        if not schema:
            return {
                "subject": subject,
                "version": version,
                "exists": False
            }
            
        return {
            "id": schema.id,
            "subject": schema.subject,
            "version": schema.version,
            "schema": schema.schema,
            "exists": True
        }
    
    async def get_schemas(
        self,
        subject: str,
        ctx: Context = None
    ) -> List[Dict[str, Any]]:
        """Get all schema versions for a subject.
        
        Args:
            subject: Subject name
            
        Returns:
            List of schema information
        """
        if ctx:
            await ctx.info(f"Getting all schema versions for subject: {subject}")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        schemas = kafka_api.get_schemas(subject)
        
        result = []
        for schema in schemas:
            result.append({
                "id": schema.id,
                "subject": schema.subject,
                "version": schema.version
            })
            
        return result
    
    async def get_subjects(self, ctx: Context = None) -> List[str]:
        """Get all Kafka schema subjects.
        
        Returns:
            List of subjects
        """
        if ctx:
            await ctx.info("Getting all Kafka schema subjects")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        subjects = kafka_api.get_subjects()
        
        return subjects
    
    async def delete_schema(
        self,
        subject: str,
        version: int,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Delete a Kafka schema.
        
        Args:
            subject: Subject name
            version: Version number
            
        Returns:
            Deletion status
        """
        if ctx:
            await ctx.info(f"Deleting Kafka schema: {subject} (version {version})")
            await ctx.info("WARNING: This operation cannot be undone")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        schema = kafka_api.get_schema(subject, version)
        
        if not schema:
            return {
                "subject": subject,
                "version": version,
                "status": "not_found"
            }
            
        schema.delete()
        
        return {
            "subject": subject,
            "version": version,
            "status": "deleted"
        }
    
    async def create_topic(
        self,
        name: str,
        schema: str,
        schema_version: int,
        replicas: int = 1,
        partitions: int = 1,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Create a new Kafka topic.
        
        Args:
            name: Name of the topic
            schema: Subject name of the schema
            schema_version: Version of the schema
            replicas: Replication factor for the topic
            partitions: Partitions for the topic
            
        Returns:
            Topic information
        """
        if ctx:
            await ctx.info(f"Creating Kafka topic: {name}")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        topic = kafka_api.create_topic(
            name=name,
            schema=schema,
            schema_version=schema_version,
            replicas=replicas,
            partitions=partitions
        )
        
        return {
            "name": topic.name,
            "partitions": topic.num_partitions,
            "replicas": topic.num_replicas,
            "schema": topic.schema,
            "status": "created"
        }
    
    async def get_topic(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Get a Kafka topic by name.
        
        Args:
            name: Name of the topic
            
        Returns:
            Topic information
        """
        if ctx:
            await ctx.info(f"Getting Kafka topic: {name}")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        topic = kafka_api.get_topic(name)
        
        if not topic:
            return {
                "name": name,
                "exists": False
            }
            
        return {
            "name": topic.name,
            "partitions": topic.num_partitions,
            "replicas": topic.num_replicas,
            "schema": topic.schema,
            "exists": True
        }
    
    async def get_topics(self, ctx: Context = None) -> List[Dict[str, Any]]:
        """Get all Kafka topics.
        
        Returns:
            List of topic information
        """
        if ctx:
            await ctx.info("Getting all Kafka topics")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        topics = kafka_api.get_topics()
        
        result = []
        for topic in topics:
            result.append({
                "name": topic.name,
                "partitions": topic.num_partitions,
                "replicas": topic.num_replicas,
                "schema": topic.schema
            })
            
        return result
    
    async def delete_topic(
        self,
        name: str,
        ctx: Context = None
    ) -> Dict[str, Any]:
        """Delete a Kafka topic.
        
        Args:
            name: Name of the topic
            
        Returns:
            Deletion status
        """
        if ctx:
            await ctx.info(f"Deleting Kafka topic: {name}")
            await ctx.info("WARNING: This operation cannot be undone")
        
        project = hopsworks.get_current_project()
        kafka_api = project.get_kafka_api()
        
        topic = kafka_api.get_topic(name)
        
        if not topic:
            return {
                "name": name,
                "status": "not_found"
            }
            
        topic.delete()
        
        return {
            "name": name,
            "status": "deleted"
        }