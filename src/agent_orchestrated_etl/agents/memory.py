"""Agent memory and state management system."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from ..exceptions import AgentException
from ..logging_config import get_logger

# Vector search dependencies (optional)
try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    SENTENCE_TRANSFORMERS_AVAILABLE = True
except ImportError:
    SENTENCE_TRANSFORMERS_AVAILABLE = False


class MemoryType(Enum):
    """Types of memory entries."""
    
    EPISODIC = "episodic"  # Specific events and experiences
    SEMANTIC = "semantic"  # General knowledge and facts
    PROCEDURAL = "procedural"  # Skills and procedures
    WORKING = "working"  # Temporary working memory
    LONG_TERM = "long_term"  # Persistent long-term memories


class MemoryImportance(Enum):
    """Importance levels for memory entries."""
    
    CRITICAL = 10
    HIGH = 8
    MEDIUM = 5
    LOW = 3
    TRIVIAL = 1


@dataclass
class MemoryEntry:
    """Represents a single memory entry."""
    
    entry_id: str
    memory_type: MemoryType
    content: Dict[str, Any]
    importance: MemoryImportance = MemoryImportance.MEDIUM
    tags: Set[str] = field(default_factory=set)
    
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    
    # Relationships
    related_entries: Set[str] = field(default_factory=set)
    parent_entry: Optional[str] = None
    
    # Metadata
    source: str = "agent"
    confidence: float = 1.0  # 0.0 to 1.0
    expires_at: Optional[float] = None
    
    def is_expired(self) -> bool:
        """Check if this memory entry has expired."""
        return self.expires_at is not None and time.time() > self.expires_at
    
    def access(self) -> None:
        """Record an access to this memory entry."""
        self.last_accessed = time.time()
        self.access_count += 1
    
    def decay_importance(self, decay_factor: float = 0.99) -> None:
        """Apply decay to importance based on time since last access."""
        time_since_access = time.time() - self.last_accessed
        
        # Decay importance based on time (daily decay)
        days_since_access = time_since_access / (24 * 60 * 60)
        decayed_importance = self.importance.value * (decay_factor ** days_since_access)
        
        # Convert back to enum (find closest value)
        for importance_level in MemoryImportance:
            if decayed_importance >= importance_level.value:
                self.importance = importance_level
                break
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert memory entry to dictionary."""
        return {
            "entry_id": self.entry_id,
            "memory_type": self.memory_type.value,
            "content": self.content,
            "importance": self.importance.value,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "last_accessed": self.last_accessed,
            "access_count": self.access_count,
            "related_entries": list(self.related_entries),
            "parent_entry": self.parent_entry,
            "source": self.source,
            "confidence": self.confidence,
            "expires_at": self.expires_at,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MemoryEntry':
        """Create memory entry from dictionary."""
        return cls(
            entry_id=data["entry_id"],
            memory_type=MemoryType(data["memory_type"]),
            content=data["content"],
            importance=MemoryImportance(data["importance"]),
            tags=set(data.get("tags", [])),
            created_at=data["created_at"],
            last_accessed=data["last_accessed"],
            access_count=data["access_count"],
            related_entries=set(data.get("related_entries", [])),
            parent_entry=data.get("parent_entry"),
            source=data.get("source", "agent"),
            confidence=data.get("confidence", 1.0),
            expires_at=data.get("expires_at"),
        )


class MemoryQuery(BaseModel):
    """Represents a query for retrieving memories."""
    
    query_text: str = Field(description="Text query for searching memories")
    memory_types: Optional[List[MemoryType]] = Field(None, description="Filter by memory types")
    tags: Optional[List[str]] = Field(None, description="Filter by tags")
    min_importance: Optional[MemoryImportance] = Field(None, description="Minimum importance level")
    max_age_hours: Optional[float] = Field(None, description="Maximum age in hours")
    min_confidence: Optional[float] = Field(None, description="Minimum confidence level")
    limit: int = Field(default=10, ge=1, le=1000, description="Maximum number of results")


class AgentMemory:
    """Memory management system for agents."""
    
    def __init__(
        self,
        agent_id: str,
        max_entries: int = 10000,
        working_memory_size: int = 100,
        cleanup_interval_hours: float = 24.0,
    ):
        self.agent_id = agent_id
        self.max_entries = max_entries
        self.working_memory_size = working_memory_size
        self.cleanup_interval_hours = cleanup_interval_hours
        
        # Memory storage
        self.memories: Dict[str, MemoryEntry] = {}
        self.working_memory: List[str] = []  # Most recently accessed entry IDs
        
        # Indexes for efficient retrieval
        self.tag_index: Dict[str, Set[str]] = {}  # tag -> set of entry_ids
        self.type_index: Dict[MemoryType, Set[str]] = {}  # memory_type -> set of entry_ids
        self.importance_index: Dict[MemoryImportance, Set[str]] = {}  # importance -> set of entry_ids
        
        # Statistics and metrics
        self.stats = {
            "total_entries": 0,
            "entries_by_type": {mt.value: 0 for mt in MemoryType},
            "entries_by_importance": {mi.value: 0 for mi in MemoryImportance},
            "total_accesses": 0,
            "last_cleanup": time.time(),
        }
        
        # Vector search components (optional)
        self._vector_search_enabled = False
        self._embedding_model = None
        self._vector_store = None
        self._similarity_threshold = 0.7  # Configurable similarity threshold
        
        # Initialize logger first
        self.logger = get_logger(f"agent.memory.{agent_id}")
        
        # Initialize vector search if dependencies are available
        self._init_vector_search()
        
        self.logger.info(f"Memory system initialized for agent {agent_id}")
        if self._vector_search_enabled:
            self.logger.info("Vector-based semantic search enabled")
        else:
            self.logger.info("Using traditional substring search (vector dependencies not available)")
    
    def store_memory(
        self,
        content: Dict[str, Any],
        memory_type: MemoryType = MemoryType.EPISODIC,
        importance: MemoryImportance = MemoryImportance.MEDIUM,
        tags: Optional[Set[str]] = None,
        entry_id: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Store a new memory entry.
        
        Args:
            content: The memory content
            memory_type: Type of memory
            importance: Importance level
            tags: Associated tags
            entry_id: Optional custom entry ID
            **kwargs: Additional memory entry parameters
            
        Returns:
            The ID of the stored memory entry
            
        Raises:
            AgentException: If memory storage fails
        """
        if entry_id is None:
            entry_id = f"{self.agent_id}_{int(time.time() * 1000000)}"
        
        if entry_id in self.memories:
            raise AgentException(f"Memory entry {entry_id} already exists")
        
        try:
            # Create memory entry
            memory_entry = MemoryEntry(
                entry_id=entry_id,
                memory_type=memory_type,
                content=content,
                importance=importance,
                tags=tags or set(),
                **kwargs,
            )
            
            # Check capacity and clean up if necessary
            if len(self.memories) >= self.max_entries:
                self._cleanup_memories(force=True)
            
            # Store memory
            self.memories[entry_id] = memory_entry
            
            # Update indexes
            self._update_indexes_on_add(memory_entry)
            
            # Store in vector database if enabled
            self._store_memory_vector(memory_entry)
            
            # Update working memory
            self._update_working_memory(entry_id)
            
            # Update statistics
            self._update_stats_on_add(memory_entry)
            
            self.logger.debug(
                f"Memory stored: {entry_id}",
                extra={
                    "memory_type": memory_type.value,
                    "importance": importance.value,
                    "tags": list(tags) if tags else [],
                }
            )
            
            return entry_id
            
        except Exception as e:
            self.logger.error(f"Failed to store memory: {e}", exc_info=e)
            raise AgentException(f"Memory storage failed: {e}") from e
    
    def _init_vector_search(self) -> None:
        """Initialize vector search components if dependencies are available."""
        if not CHROMADB_AVAILABLE or not SENTENCE_TRANSFORMERS_AVAILABLE:
            self.logger.info("Vector search dependencies not available, using substring search")
            return
        
        try:
            # Initialize embedding model
            self._init_embedding_model()
            
            # Initialize vector store
            self._init_vector_store()
            
            self._vector_search_enabled = True
            self.logger.info("Vector search initialized successfully")
            
        except Exception as e:
            self.logger.warning(f"Failed to initialize vector search: {e}, falling back to substring search")
            self._vector_search_enabled = False
    
    def _init_embedding_model(self) -> None:
        """Initialize the sentence transformer model for embeddings."""
        try:
            # Use a lightweight, fast model suitable for semantic search
            model_name = "all-MiniLM-L6-v2"  # 384-dimensional embeddings, fast and effective
            self._embedding_model = SentenceTransformer(model_name)
            self.logger.debug(f"Embedding model '{model_name}' loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load embedding model: {e}")
            raise
    
    def _init_vector_store(self) -> None:
        """Initialize ChromaDB vector store."""
        try:
            # Create or connect to ChromaDB client
            # Use persistent client for production, in-memory for testing
            client = chromadb.Client(Settings(
                is_persistent=True,
                persist_directory=f"./chroma_db/agent_{self.agent_id}",
                anonymized_telemetry=False
            ))
            
            # Get or create collection for this agent's memories
            collection_name = f"memories_{self.agent_id}"
            self._vector_store = client.get_or_create_collection(
                name=collection_name,
                metadata={"description": f"Semantic memory for agent {self.agent_id}"}
            )
            
            self.logger.debug(f"Vector store collection '{collection_name}' initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize vector store: {e}")
            raise
    
    def _generate_embedding(self, content: Dict[str, Any]) -> List[float]:
        """Generate embedding vector for memory content."""
        if not self._embedding_model:
            raise AgentException("Embedding model not initialized")
        
        # Extract text content for embedding
        text_content = self._extract_text_for_embedding_from_dict(content)
        
        # Generate embedding
        embedding = self._embedding_model.encode(text_content)
        # Handle both numpy arrays and lists
        if hasattr(embedding, 'tolist'):
            return embedding.tolist()
        else:
            return list(embedding)
    
    def _extract_text_for_embedding(self, memory: MemoryEntry) -> str:
        """Extract relevant text content from memory entry for embedding generation."""
        text_parts = []
        
        # Extract content text
        content_text = self._extract_text_for_embedding_from_dict(memory.content)
        if content_text:
            text_parts.append(content_text)
        
        # Include tags as context
        if memory.tags:
            tags_text = " ".join(memory.tags)
            text_parts.append(f"tags: {tags_text}")
        
        # Include memory type as context
        text_parts.append(f"type: {memory.memory_type.value}")
        
        return " ".join(text_parts)
    
    def _extract_text_for_embedding_from_dict(self, content: Dict[str, Any]) -> str:
        """Extract text content from a dictionary for embedding."""
        text_parts = []
        
        def extract_text_recursive(obj, max_depth=3, current_depth=0):
            if current_depth >= max_depth:
                return
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str) and value.strip():
                        text_parts.append(value.strip())
                    elif isinstance(value, (dict, list)):
                        extract_text_recursive(value, max_depth, current_depth + 1)
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, str) and item.strip():
                        text_parts.append(item.strip())
                    elif isinstance(item, (dict, list)):
                        extract_text_recursive(item, max_depth, current_depth + 1)
            elif isinstance(obj, str) and obj.strip():
                text_parts.append(obj.strip())
        
        extract_text_recursive(content)
        return " ".join(text_parts)
    
    def _store_memory_vector(self, memory: MemoryEntry) -> None:
        """Store memory in vector database."""
        if not self._vector_search_enabled or not self._vector_store:
            return
        
        try:
            # Generate embedding
            embedding = self._generate_embedding(memory.content)
            
            # Extract text for metadata
            text_content = self._extract_text_for_embedding(memory)
            
            # Store in vector database
            self._vector_store.add(
                ids=[memory.entry_id],
                embeddings=[embedding],
                metadatas=[{
                    "entry_id": memory.entry_id,
                    "memory_type": memory.memory_type.value,
                    "importance": memory.importance.value,
                    "created_at": memory.created_at,
                    "tags": list(memory.tags),
                    "text_content": text_content[:1000]  # Limit metadata size
                }],
                documents=[text_content]
            )
            
            self.logger.debug(f"Memory vector stored for entry {memory.entry_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to store memory vector: {e}")
            # Don't raise exception - vector storage is optional
    
    def _vector_similarity_search(
        self, 
        query: MemoryQuery, 
        similarity_threshold: Optional[float] = None
    ) -> List[MemoryEntry]:
        """Perform vector similarity search for memories."""
        if not self._vector_search_enabled or not self._vector_store or not query.query_text:
            return []
        
        try:
            # Generate query embedding
            query_embedding = self._embedding_model.encode(query.query_text)
            
            # Handle both numpy arrays and lists
            if hasattr(query_embedding, 'tolist'):
                embedding_list = query_embedding.tolist()
            else:
                embedding_list = list(query_embedding)
            
            # Perform similarity search
            results = self._vector_store.query(
                query_embeddings=[embedding_list],
                n_results=min(query.limit * 2, 100),  # Get more results for filtering
                include=["metadatas", "distances"]
            )
            
            # Process results and apply similarity threshold
            threshold = similarity_threshold or self._similarity_threshold
            matched_memories = []
            
            if results['ids'] and results['ids'][0]:
                for i, (entry_id, distance) in enumerate(zip(results['ids'][0], results['distances'][0])):
                    # Lower distance = higher similarity; convert to similarity score
                    similarity = 1.0 - distance
                    
                    if similarity >= threshold:
                        # Retrieve actual memory entry
                        memory = self.memories.get(entry_id)
                        if memory and not memory.is_expired():
                            matched_memories.append(memory)
            
            # Sort by similarity (already ordered by ChromaDB, but ensure consistency)
            return matched_memories[:query.limit]
            
        except Exception as e:
            self.logger.error(f"Vector similarity search failed: {e}")
            return []
    
    def retrieve_memory(self, entry_id: str) -> Optional[MemoryEntry]:
        """Retrieve a specific memory entry by ID.
        
        Args:
            entry_id: ID of the memory entry
            
        Returns:
            Memory entry if found, None otherwise
        """
        memory = self.memories.get(entry_id)
        if memory:
            if memory.is_expired():
                self._remove_memory(entry_id)
                return None
            
            memory.access()
            self._update_working_memory(entry_id)
            self.stats["total_accesses"] += 1
            
        return memory
    
    def search_memories(self, query: MemoryQuery) -> List[MemoryEntry]:
        """Search for memories based on query criteria.
        
        Args:
            query: Search query parameters
            
        Returns:
            List of matching memory entries
        """
        try:
            # Use vector search for text queries when available, otherwise use traditional approach
            if self._vector_search_enabled and query.query_text:
                # Get vector similarity results first
                vector_results = self._vector_similarity_search(query)
                vector_result_ids = {memory.entry_id for memory in vector_results}
                
                # Start with vector search results, then apply additional filters
                candidate_ids = vector_result_ids
                
                # If no vector results but traditional filters exist, fall back to all memories
                if not candidate_ids and (query.memory_types or query.tags or query.min_importance):
                    candidate_ids = set(self.memories.keys())
            else:
                # Traditional approach: start with all memories
                candidate_ids = set(self.memories.keys())
            
            # Apply traditional filters
            if query.memory_types:
                type_ids = set()
                for memory_type in query.memory_types:
                    type_ids.update(self.type_index.get(memory_type, set()))
                candidate_ids &= type_ids
            
            if query.tags:
                tag_ids = set()
                for tag in query.tags:
                    tag_ids.update(self.tag_index.get(tag, set()))
                candidate_ids &= tag_ids
            
            if query.min_importance:
                importance_ids = set()
                for importance in MemoryImportance:
                    if importance.value >= query.min_importance.value:
                        importance_ids.update(self.importance_index.get(importance, set()))
                candidate_ids &= importance_ids
            
            # Apply additional filters
            filtered_memories = []
            current_time = time.time()
            
            for entry_id in candidate_ids:
                memory = self.memories.get(entry_id)
                if not memory or memory.is_expired():
                    continue
                
                # Age filter
                if query.max_age_hours:
                    age_hours = (current_time - memory.created_at) / 3600
                    if age_hours > query.max_age_hours:
                        continue
                
                # Confidence filter
                if query.min_confidence and memory.confidence < query.min_confidence:
                    continue
                
                # Traditional text search (only if vector search wasn't used)
                if query.query_text and not self._vector_search_enabled:
                    content_text = json.dumps(memory.content).lower()
                    if query.query_text.lower() not in content_text:
                        continue
                
                filtered_memories.append(memory)
            
            # Sort by relevance (importance + recency + access frequency + vector similarity)
            def relevance_score(mem: MemoryEntry) -> float:
                recency = 1.0 / (1.0 + (current_time - mem.last_accessed) / 3600)  # Recency in hours
                frequency = min(mem.access_count / 100.0, 1.0)  # Normalized frequency
                importance = mem.importance.value / 10.0  # Normalized importance
                
                # If vector search was used, preserve the similarity-based ordering
                if self._vector_search_enabled and query.query_text and mem.entry_id in vector_result_ids:
                    # Vector results are already ordered by similarity, so use position as similarity score
                    vector_position = next((i for i, vm in enumerate(vector_results) if vm.entry_id == mem.entry_id), len(vector_results))
                    vector_similarity = 1.0 - (vector_position / max(len(vector_results), 1))  # Higher for earlier results
                    return importance * 0.3 + recency * 0.2 + frequency * 0.1 + vector_similarity * 0.4
                else:
                    return importance * 0.5 + recency * 0.3 + frequency * 0.2
            
            filtered_memories.sort(key=relevance_score, reverse=True)
            
            # Access the retrieved memories
            for memory in filtered_memories[:query.limit]:
                memory.access()
                self._update_working_memory(memory.entry_id)
            
            self.stats["total_accesses"] += len(filtered_memories[:query.limit])
            
            return filtered_memories[:query.limit]
            
        except Exception as e:
            self.logger.error(f"Memory search failed: {e}", exc_info=e)
            return []
    
    def update_memory(
        self,
        entry_id: str,
        content: Optional[Dict[str, Any]] = None,
        importance: Optional[MemoryImportance] = None,
        tags: Optional[Set[str]] = None,
        confidence: Optional[float] = None,
    ) -> bool:
        """Update an existing memory entry.
        
        Args:
            entry_id: ID of the memory entry to update
            content: New content (optional)
            importance: New importance level (optional)
            tags: New tags (optional)
            confidence: New confidence level (optional)
            
        Returns:
            True if update successful, False otherwise
        """
        memory = self.memories.get(entry_id)
        if not memory:
            self.logger.warning(f"Attempted to update non-existent memory: {entry_id}")
            return False
        
        try:
            # Remove from old indexes
            self._update_indexes_on_remove(memory)
            
            # Update fields
            if content is not None:
                memory.content = content
            if importance is not None:
                old_importance = memory.importance
                memory.importance = importance
                # Update stats
                self.stats["entries_by_importance"][old_importance.value] -= 1
                self.stats["entries_by_importance"][importance.value] += 1
            if tags is not None:
                memory.tags = tags
            if confidence is not None:
                memory.confidence = confidence
            
            # Update access info
            memory.access()
            
            # Add back to indexes
            self._update_indexes_on_add(memory)
            
            # Update working memory
            self._update_working_memory(entry_id)
            
            self.logger.debug(f"Memory updated: {entry_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update memory {entry_id}: {e}", exc_info=e)
            return False
    
    def delete_memory(self, entry_id: str) -> bool:
        """Delete a memory entry.
        
        Args:
            entry_id: ID of the memory entry to delete
            
        Returns:
            True if deletion successful, False otherwise
        """
        if entry_id not in self.memories:
            return False
        
        try:
            self._remove_memory(entry_id)
            self.logger.debug(f"Memory deleted: {entry_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete memory {entry_id}: {e}", exc_info=e)
            return False
    
    def get_working_memory(self) -> List[MemoryEntry]:
        """Get the current working memory entries."""
        working_memories = []
        for entry_id in self.working_memory:
            memory = self.memories.get(entry_id)
            if memory and not memory.is_expired():
                working_memories.append(memory)
        
        return working_memories
    
    def clear_working_memory(self) -> None:
        """Clear the working memory."""
        self.working_memory.clear()
        self.logger.info("Working memory cleared")
    
    def get_memory_summary(self) -> Dict[str, Any]:
        """Get a summary of the memory system status."""
        return {
            "agent_id": self.agent_id,
            "total_memories": len(self.memories),
            "working_memory_size": len(self.working_memory),
            "memory_types": {mt.value: len(self.type_index.get(mt, set())) for mt in MemoryType},
            "importance_distribution": {
                mi.value: len(self.importance_index.get(mi, set())) for mi in MemoryImportance
            },
            "stats": self.stats.copy(),
        }
    
    def cleanup_memories(self) -> int:
        """Perform memory cleanup and return number of entries removed."""
        return self._cleanup_memories()
    
    def _update_indexes_on_add(self, memory: MemoryEntry) -> None:
        """Update indexes when adding a memory."""
        # Tag index
        for tag in memory.tags:
            if tag not in self.tag_index:
                self.tag_index[tag] = set()
            self.tag_index[tag].add(memory.entry_id)
        
        # Type index
        if memory.memory_type not in self.type_index:
            self.type_index[memory.memory_type] = set()
        self.type_index[memory.memory_type].add(memory.entry_id)
        
        # Importance index
        if memory.importance not in self.importance_index:
            self.importance_index[memory.importance] = set()
        self.importance_index[memory.importance].add(memory.entry_id)
    
    def _update_indexes_on_remove(self, memory: MemoryEntry) -> None:
        """Update indexes when removing a memory."""
        # Tag index
        for tag in memory.tags:
            if tag in self.tag_index:
                self.tag_index[tag].discard(memory.entry_id)
                if not self.tag_index[tag]:
                    del self.tag_index[tag]
        
        # Type index
        if memory.memory_type in self.type_index:
            self.type_index[memory.memory_type].discard(memory.entry_id)
            if not self.type_index[memory.memory_type]:
                del self.type_index[memory.memory_type]
        
        # Importance index
        if memory.importance in self.importance_index:
            self.importance_index[memory.importance].discard(memory.entry_id)
            if not self.importance_index[memory.importance]:
                del self.importance_index[memory.importance]
    
    def _update_working_memory(self, entry_id: str) -> None:
        """Update working memory with recently accessed entry."""
        # Remove if already present
        if entry_id in self.working_memory:
            self.working_memory.remove(entry_id)
        
        # Add to front
        self.working_memory.insert(0, entry_id)
        
        # Trim to size
        if len(self.working_memory) > self.working_memory_size:
            self.working_memory = self.working_memory[:self.working_memory_size]
    
    def _update_stats_on_add(self, memory: MemoryEntry) -> None:
        """Update statistics when adding a memory."""
        self.stats["total_entries"] += 1
        self.stats["entries_by_type"][memory.memory_type.value] += 1
        self.stats["entries_by_importance"][memory.importance.value] += 1
    
    def _update_stats_on_remove(self, memory: MemoryEntry) -> None:
        """Update statistics when removing a memory."""
        self.stats["total_entries"] -= 1
        self.stats["entries_by_type"][memory.memory_type.value] -= 1
        self.stats["entries_by_importance"][memory.importance.value] -= 1
    
    def _remove_memory(self, entry_id: str) -> None:
        """Remove a memory entry and update all indexes."""
        memory = self.memories.get(entry_id)
        if not memory:
            return
        
        # Update indexes
        self._update_indexes_on_remove(memory)
        
        # Remove from working memory
        if entry_id in self.working_memory:
            self.working_memory.remove(entry_id)
        
        # Update stats
        self._update_stats_on_remove(memory)
        
        # Remove from storage
        del self.memories[entry_id]
    
    def _cleanup_memories(self, force: bool = False) -> int:
        """Clean up expired and low-importance memories."""
        current_time = time.time()
        
        # Check if cleanup is needed
        if not force:
            time_since_cleanup = current_time - self.stats["last_cleanup"]
            if time_since_cleanup < self.cleanup_interval_hours * 3600:
                return 0
        
        removed_count = 0
        to_remove = []
        
        try:
            for entry_id, memory in self.memories.items():
                # Remove expired memories
                if memory.is_expired():
                    to_remove.append(entry_id)
                    continue
                
                # Apply importance decay
                memory.decay_importance()
                
                # Remove very low importance memories if at capacity
                if force and len(self.memories) >= self.max_entries:
                    if memory.importance == MemoryImportance.TRIVIAL:
                        # Also consider age and access frequency
                        age_days = (current_time - memory.created_at) / (24 * 60 * 60)
                        if age_days > 30 and memory.access_count < 2:
                            to_remove.append(entry_id)
            
            # Remove identified memories
            for entry_id in to_remove:
                self._remove_memory(entry_id)
                removed_count += 1
            
            self.stats["last_cleanup"] = current_time
            
            if removed_count > 0:
                self.logger.info(f"Memory cleanup completed: {removed_count} entries removed")
            
            return removed_count
            
        except Exception as e:
            self.logger.error(f"Error during memory cleanup: {e}", exc_info=e)
            return removed_count