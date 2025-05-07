"""Embeddings capability for Hopsworks MCP server."""

from typing import Dict, Any, List, Optional, Union, Literal
import json
import hopsworks
from fastmcp import Context


class EmbeddingTools:
    """Tools for working with Hopsworks embedding features and indexes."""

    def __init__(self, mcp):
        """Initialize embedding tools.
        
        Args:
            mcp: The MCP server instance
        """
        self.mcp = mcp
        
        @self.mcp.tool()
        async def create_embedding_index(
            index_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a new embedding index for vector similarity search.
            
            Args:
                index_name: Name of the embedding index (defaults to project index if not specified)
                
            Returns:
                dict: Embedding index information
            """
            if ctx:
                await ctx.info(f"Creating embedding index: {index_name or 'default project index'}")
            
            try:
                import hsfs.embedding
                
                # Create embedding index
                embedding_index = hsfs.embedding.EmbeddingIndex(index_name=index_name)
                
                return {
                    "index_name": index_name or "default project index",
                    "embeddings_count": 0,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create embedding index: {str(e)}"
                }
        
        @self.mcp.tool()
        async def add_embedding_to_index(
            dimension: int,
            name: str,
            similarity_function: Literal["l2_norm", "cosine", "dot_product"] = "l2_norm",
            index_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Add an embedding feature to an embedding index.
            
            Args:
                dimension: Dimension of the embedding vector
                name: Name of the embedding feature
                similarity_function: Type of similarity function to use
                index_name: Name of the embedding index (defaults to project index if not specified)
                
            Returns:
                dict: Embedding feature information
            """
            if ctx:
                await ctx.info(f"Adding embedding '{name}' with dimension {dimension} to index {index_name or 'default project index'}")
            
            try:
                import hsfs.embedding
                
                # Create embedding index
                embedding_index = hsfs.embedding.EmbeddingIndex(index_name=index_name)
                
                # Map similarity function
                if similarity_function == "l2_norm":
                    sim_func = hsfs.embedding.SimilarityFunctionType.L2
                elif similarity_function == "cosine":
                    sim_func = hsfs.embedding.SimilarityFunctionType.COSINE
                elif similarity_function == "dot_product":
                    sim_func = hsfs.embedding.SimilarityFunctionType.DOT_PRODUCT
                else:
                    sim_func = hsfs.embedding.SimilarityFunctionType.L2
                
                # Add embedding to the index
                embedding_index.add_embedding(
                    name=name,
                    dimension=dimension,
                    similarity_function_type=sim_func
                )
                
                # Get the embedding back to verify
                embedding = embedding_index.get_embedding(name)
                
                return {
                    "index_name": index_name or "default project index",
                    "name": embedding.name if embedding else name,
                    "dimension": embedding.dimension if embedding else dimension,
                    "similarity_function": similarity_function,
                    "status": "added"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to add embedding to index: {str(e)}"
                }
                
        @self.mcp.tool()
        async def create_feature_group_with_embedding(
            name: str,
            version: Optional[int] = None,
            primary_key: Optional[List[str]] = None,
            embedding_name: str = "embedding",
            embedding_dimension: int = 768,
            similarity_function: Literal["l2_norm", "cosine", "dot_product"] = "l2_norm",
            description: str = "",
            time_travel_format: str = "HUDI",
            online_enabled: bool = True,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Create a feature group with an embedding index for vector similarity search.
            
            Args:
                name: Name of the feature group
                version: Version of the feature group (defaults to incremented from last)
                primary_key: List of feature names to use as primary key
                embedding_name: Name of the embedding feature
                embedding_dimension: Dimension of the embedding vector
                similarity_function: Type of similarity function to use
                description: Description of the feature group
                time_travel_format: Time travel format to use
                online_enabled: Whether to enable online storage
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Feature group with embedding index information
            """
            if ctx:
                await ctx.info(f"Creating feature group '{name}' with embedding index for '{embedding_name}'")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                import hsfs.embedding
                
                # Create embedding index
                embedding_index = hsfs.embedding.EmbeddingIndex()
                
                # Map similarity function
                if similarity_function == "l2_norm":
                    sim_func = hsfs.embedding.SimilarityFunctionType.L2
                elif similarity_function == "cosine":
                    sim_func = hsfs.embedding.SimilarityFunctionType.COSINE
                elif similarity_function == "dot_product":
                    sim_func = hsfs.embedding.SimilarityFunctionType.DOT_PRODUCT
                else:
                    sim_func = hsfs.embedding.SimilarityFunctionType.L2
                
                # Add embedding to the index
                embedding_index.add_embedding(
                    name=embedding_name,
                    dimension=embedding_dimension,
                    similarity_function_type=sim_func
                )
                
                # Create feature group with embedding index
                feature_group = fs.create_feature_group(
                    name=name,
                    version=version,
                    description=description,
                    primary_key=primary_key,
                    time_travel_format=time_travel_format,
                    online_enabled=online_enabled,
                    embedding_index=embedding_index
                )
                
                # Save the feature group - this is just the schema, not inserting data
                feature_group.save()
                
                return {
                    "name": feature_group.name,
                    "version": feature_group.version,
                    "description": feature_group.description,
                    "embedding_name": embedding_name,
                    "embedding_dimension": embedding_dimension,
                    "similarity_function": similarity_function,
                    "primary_key": primary_key,
                    "online_enabled": online_enabled,
                    "status": "created"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to create feature group with embedding: {str(e)}"
                }
                
        @self.mcp.tool()
        async def insert_embedding_vectors(
            feature_group_name: str,
            data: str,  # JSON string of records including embedding vectors
            feature_group_version: int = 1,
            embedding_column: Optional[str] = None,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Insert data with embedding vectors into a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                data: JSON string of records including embedding vectors
                feature_group_version: Version of the feature group
                embedding_column: Name of the embedding column (if not specified, uses the first embedding in the index)
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Status information
            """
            if ctx:
                await ctx.info(f"Inserting embedding vectors into feature group: {feature_group_name} (v{feature_group_version})")
            
            try:
                import pandas as pd
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get the feature group
                fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Parse the JSON data
                try:
                    df = pd.read_json(data, orient='records')
                except Exception as e:
                    return {
                        "status": "error",
                        "message": f"Failed to parse JSON data: {str(e)}"
                    }
                
                # If embedding_column not specified, try to determine it from the embedding index
                if not embedding_column:
                    if hasattr(fg, 'embedding_index') and fg.embedding_index:
                        embeddings = fg.embedding_index.get_embeddings()
                        if embeddings and len(embeddings) > 0:
                            embedding_column = embeddings[0].name
                
                # Insert the data
                fg.insert(df)
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "rows_inserted": len(df),
                    "embedding_column": embedding_column,
                    "status": "inserted"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to insert embedding vectors: {str(e)}"
                }
                
        @self.mcp.tool()
        async def find_similar_vectors(
            feature_group_name: str,
            embedding_vector: List[float],
            k: int = 10,
            embedding_column: Optional[str] = None,
            filter_expression: Optional[str] = None,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Find the k most similar vectors to the query vector in a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                embedding_vector: Query embedding vector as list of floats
                k: Number of similar vectors to return
                embedding_column: Name of the embedding column (if not specified, uses the first embedding in the index)
                filter_expression: Filter expression to apply to the search (e.g. "id > 10")
                feature_group_version: Version of the feature group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Similar vectors with their similarity scores
            """
            if ctx:
                await ctx.info(f"Finding {k} similar vectors in feature group: {feature_group_name} (v{feature_group_version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get the feature group
                fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                
                # If embedding_column not specified, try to determine it from the embedding index
                if not embedding_column:
                    if hasattr(fg, 'embedding_index') and fg.embedding_index:
                        embeddings = fg.embedding_index.get_embeddings()
                        if embeddings and len(embeddings) > 0:
                            embedding_column = embeddings[0].name
                
                # Prepare filter if provided
                filter_obj = None
                if filter_expression:
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
                    except Exception as filter_error:
                        return {
                            "status": "error",
                            "message": f"Failed to parse filter expression: {str(filter_error)}"
                        }
                
                # Find similar vectors
                neighbors = fg.find_neighbors(
                    embedding=embedding_vector,
                    col=embedding_column,
                    k=k,
                    filter=filter_obj
                )
                
                # Format the results
                results = []
                for similarity_score, values in neighbors:
                    result = {"similarity_score": similarity_score}
                    # Add all the feature values
                    for i, feature in enumerate(fg.schema):
                        result[feature.name] = values[i]
                    results.append(result)
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "embedding_column": embedding_column,
                    "query_vector_dimension": len(embedding_vector),
                    "results_count": len(results),
                    "results": results,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to find similar vectors: {str(e)}"
                }
                
        @self.mcp.tool()
        async def get_embedding_index_info(
            feature_group_name: str,
            feature_group_version: int = 1,
            project_name: Optional[str] = None,
            ctx: Context = None
        ) -> Dict[str, Any]:
            """Get information about the embedding index for a feature group.
            
            Args:
                feature_group_name: Name of the feature group
                feature_group_version: Version of the feature group
                project_name: Name of the Hopsworks project
                
            Returns:
                dict: Embedding index information
            """
            if ctx:
                await ctx.info(f"Getting embedding index info for feature group: {feature_group_name} (v{feature_group_version})")
            
            try:
                project = hopsworks.get_current_project()
                fs = project.get_feature_store(name=project_name)
                
                # Get the feature group
                fg = fs.get_feature_group(name=feature_group_name, version=feature_group_version)
                
                # Check if the feature group has an embedding index
                if not hasattr(fg, 'embedding_index') or not fg.embedding_index:
                    return {
                        "feature_group": feature_group_name,
                        "feature_group_version": feature_group_version,
                        "status": "no_embedding_index",
                        "message": "Feature group does not have an embedding index"
                    }
                
                # Get embeddings info
                embeddings = fg.embedding_index.get_embeddings()
                embeddings_info = []
                
                for emb in embeddings:
                    emb_info = {
                        "name": emb.name,
                        "dimension": emb.dimension,
                        "similarity_function": str(emb.similarity_function_type)
                    }
                    embeddings_info.append(emb_info)
                
                # Count documents in the index (if available)
                doc_count = None
                try:
                    doc_count = fg.embedding_index.count()
                except:
                    pass
                
                return {
                    "feature_group": feature_group_name,
                    "feature_group_version": feature_group_version,
                    "index_name": fg.embedding_index.index_name if hasattr(fg.embedding_index, 'index_name') else None,
                    "embeddings": embeddings_info,
                    "document_count": doc_count,
                    "status": "success"
                }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to get embedding index info: {str(e)}"
                }