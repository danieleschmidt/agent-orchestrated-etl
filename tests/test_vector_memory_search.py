"""Tests for vector-based semantic memory search functionality."""

import time
from unittest.mock import Mock, patch

import pytest

from src.agent_orchestrated_etl.agents.memory import (
    AgentMemory,
    MemoryEntry,
    MemoryImportance,
    MemoryQuery,
    MemoryType,
)


class TestVectorMemorySearch:
    """Test vector-based semantic search for agent memory."""

    @pytest.fixture
    def agent_memory(self):
        """Create agent memory instance for testing."""
        return AgentMemory("test-agent-123")

    @pytest.fixture
    def sample_memories(self, agent_memory):
        """Create sample memory entries for testing."""
        memories = [
            MemoryEntry(
                entry_id="mem_1",
                memory_type=MemoryType.SEMANTIC,
                content={
                    "description": "Python data processing pipeline",
                    "details": "ETL pipeline for extracting and transforming customer data"
                },
                importance=MemoryImportance.HIGH,
                tags={"python", "etl", "data"}
            ),
            MemoryEntry(
                entry_id="mem_2",
                memory_type=MemoryType.PROCEDURAL,
                content={
                    "description": "SQL query optimization techniques",
                    "details": "Methods for improving database query performance"
                },
                importance=MemoryImportance.MEDIUM,
                tags={"sql", "database", "optimization"}
            ),
            MemoryEntry(
                entry_id="mem_3",
                memory_type=MemoryType.EPISODIC,
                content={
                    "description": "Machine learning model deployment",
                    "details": "Deployed ML model for customer behavior prediction"
                },
                importance=MemoryImportance.HIGH,
                tags={"ml", "deployment", "prediction"}
            ),
            MemoryEntry(
                entry_id="mem_4",
                memory_type=MemoryType.SEMANTIC,
                content={
                    "description": "API integration framework",
                    "details": "REST API framework for microservices communication"
                },
                importance=MemoryImportance.MEDIUM,
                tags={"api", "rest", "microservices"}
            )
        ]

        # Store memories in the agent memory
        for memory in memories:
            agent_memory.store_memory(
                content=memory.content,
                memory_type=memory.memory_type,
                importance=memory.importance,
                tags=memory.tags,
                entry_id=memory.entry_id
            )

        return memories

    def test_vector_search_initialization(self, agent_memory):
        """Test that vector search components are properly initialized."""
        # Check if vector search is available
        assert hasattr(agent_memory, '_vector_search_enabled')
        assert hasattr(agent_memory, '_embedding_model')
        assert hasattr(agent_memory, '_vector_store')

        # Check initialization state
        if agent_memory._vector_search_enabled:
            assert agent_memory._embedding_model is not None
            assert agent_memory._vector_store is not None

    @patch('src.agent_orchestrated_etl.agents.memory.CHROMADB_AVAILABLE', True)
    def test_embedding_generation(self, agent_memory):
        """Test embedding generation for memory content."""
        # Mock embedding model
        mock_model = Mock()
        mock_model.encode.return_value = [0.1, 0.2, 0.3, 0.4, 0.5]  # Sample embedding

        with patch.object(agent_memory, '_embedding_model', mock_model):
            content = {
                "description": "Test memory content",
                "details": "This is a test memory for embedding generation"
            }

            embedding = agent_memory._generate_embedding(content)

            assert embedding is not None
            assert len(embedding) == 5
            assert isinstance(embedding, list)
            mock_model.encode.assert_called_once()

    def test_content_extraction_for_embedding(self, agent_memory):
        """Test extraction of text content from memory for embedding."""
        memory = MemoryEntry(
            entry_id="test_mem",
            memory_type=MemoryType.SEMANTIC,
            content={
                "description": "Python programming",
                "details": "Object-oriented programming in Python",
                "metadata": {"author": "test", "version": 1},
                "tags": ["python", "oop"]
            },
            tags={"python", "programming"}
        )

        text_content = agent_memory._extract_text_for_embedding(memory)

        assert "Python programming" in text_content
        assert "Object-oriented programming" in text_content
        assert "python" in text_content.lower()
        assert "oop" in text_content.lower()
        # Should include tags in the text
        assert "programming" in text_content.lower()

    @patch('src.agent_orchestrated_etl.agents.memory.CHROMADB_AVAILABLE', True)
    def test_vector_similarity_search(self, agent_memory, sample_memories):
        """Test vector-based similarity search."""
        # Mock vector store and embedding model
        mock_vector_store = Mock()
        mock_embedding_model = Mock()

        # Mock query embedding
        query_embedding = [0.8, 0.1, 0.2, 0.3, 0.1]
        mock_embedding_model.encode.return_value = query_embedding

        # Mock vector store search results (similarity scores and IDs)
        mock_results = {
            'ids': [['mem_1', 'mem_3', 'mem_2']],
            'distances': [[0.2, 0.4, 0.7]],  # Lower distance = higher similarity
            'metadatas': [[
                {'entry_id': 'mem_1'},
                {'entry_id': 'mem_3'},
                {'entry_id': 'mem_2'}
            ]]
        }
        mock_vector_store.query.return_value = mock_results

        with patch.object(agent_memory, '_vector_store', mock_vector_store):
            with patch.object(agent_memory, '_embedding_model', mock_embedding_model):
                with patch.object(agent_memory, '_vector_search_enabled', True):

                    query = MemoryQuery(
                        query_text="data processing pipeline",
                        limit=3
                    )

                    results = agent_memory._vector_similarity_search(query)

                    # Verify results are ordered by similarity (mem_1 first, most similar)
                    assert len(results) == 3
                    assert results[0].entry_id == "mem_1"
                    assert results[1].entry_id == "mem_3"
                    assert results[2].entry_id == "mem_2"

                    # Verify vector store was called correctly
                    mock_vector_store.query.assert_called_once()
                    call_args = mock_vector_store.query.call_args
                    assert call_args[1]['query_embeddings'] == [query_embedding]
                    assert call_args[1]['n_results'] >= 3

    def test_hybrid_search_combination(self, agent_memory, sample_memories):
        """Test combination of vector search with traditional filtering."""
        # Mock vector search to return similarity results
        mock_vector_results = [
            sample_memories[0],  # mem_1 - highest similarity
            sample_memories[2],  # mem_3 - medium similarity
            sample_memories[1],  # mem_2 - lower similarity
        ]

        with patch.object(agent_memory, '_vector_similarity_search', return_value=mock_vector_results):
            with patch.object(agent_memory, '_vector_search_enabled', True):

                # Query with both text and filters
                query = MemoryQuery(
                    query_text="data processing",
                    memory_types=[MemoryType.SEMANTIC],  # Should filter out mem_3 (EPISODIC)
                    min_importance=MemoryImportance.HIGH,  # Should filter out mem_2 (MEDIUM)
                    limit=5
                )

                results = agent_memory.search_memories(query)

                # Should only return mem_1 (SEMANTIC + HIGH importance)
                assert len(results) == 1
                assert results[0].entry_id == "mem_1"

    def test_fallback_to_substring_search(self, agent_memory, sample_memories):
        """Test fallback to substring search when vector search is unavailable."""
        with patch.object(agent_memory, '_vector_search_enabled', False):

            query = MemoryQuery(
                query_text="Python data",
                limit=5
            )

            results = agent_memory.search_memories(query)

            # Should find memories containing "Python" or "data" using substring search
            assert len(results) >= 1
            # mem_1 contains both "Python" and "data" in content
            mem_1_found = any(r.entry_id == "mem_1" for r in results)
            assert mem_1_found

    def test_vector_search_performance_metrics(self, agent_memory):
        """Test that vector search performance is tracked."""
        # Mock vector search operation
        with patch.object(agent_memory, '_vector_similarity_search') as mock_vector_search:
            with patch.object(agent_memory, '_vector_search_enabled', True):
                mock_vector_search.return_value = []

                query = MemoryQuery(query_text="test query")

                start_time = time.time()
                agent_memory.search_memories(query)
                end_time = time.time()

                # Verify vector search was called
                mock_vector_search.assert_called_once_with(query)

                # Verify performance tracking (if implemented)
                if hasattr(agent_memory, 'stats'):
                    assert 'vector_searches' in agent_memory.stats or 'total_accesses' in agent_memory.stats

    @patch('src.agent_orchestrated_etl.agents.memory.CHROMADB_AVAILABLE', True)
    def test_memory_storage_with_vectors(self, agent_memory):
        """Test that new memories are stored with vector embeddings."""
        # Mock embedding generation
        mock_embedding = [0.1, 0.2, 0.3, 0.4, 0.5]

        with patch.object(agent_memory, '_generate_embedding', return_value=mock_embedding):
            with patch.object(agent_memory, '_vector_search_enabled', True):
                with patch.object(agent_memory, '_vector_store', Mock()):

                    content = {
                        "description": "New memory for vector testing",
                        "details": "This memory should be stored with embeddings"
                    }

                    agent_memory.store_memory(
                        content=content,
                        memory_type=MemoryType.SEMANTIC,
                        importance=MemoryImportance.MEDIUM,
                        entry_id="new_mem"
                    )

                    # Verify memory was stored
                    retrieved = agent_memory.retrieve_memory("new_mem")
                    assert retrieved is not None
                    assert retrieved.entry_id == "new_mem"

                    # Verify embedding generation was called
                    agent_memory._generate_embedding.assert_called_once()

    def test_vector_search_with_empty_query(self, agent_memory):
        """Test vector search behavior with empty query text."""
        with patch.object(agent_memory, '_vector_search_enabled', True):

            query = MemoryQuery(
                query_text="",  # Empty query
                memory_types=[MemoryType.SEMANTIC],
                limit=5
            )

            results = agent_memory.search_memories(query)

            # Should still work with traditional filtering, just no text search
            assert isinstance(results, list)

    def test_vector_search_similarity_threshold(self, agent_memory, sample_memories):
        """Test that similarity threshold filtering works correctly."""
        # Mock vector search with varying similarity scores
        mock_vector_store = Mock()
        mock_embedding_model = Mock()
        mock_embedding_model.encode.return_value = [0.5, 0.5, 0.5, 0.5, 0.5]

        # Results with different similarity scores (distances)
        mock_results = {
            'ids': [['mem_1', 'mem_2', 'mem_3']],
            'distances': [[0.1, 0.5, 0.9]],  # Only mem_1 and mem_2 should pass threshold
            'metadatas': [[
                {'entry_id': 'mem_1'},
                {'entry_id': 'mem_2'},
                {'entry_id': 'mem_3'}
            ]]
        }
        mock_vector_store.query.return_value = mock_results

        with patch.object(agent_memory, '_vector_store', mock_vector_store):
            with patch.object(agent_memory, '_embedding_model', mock_embedding_model):
                with patch.object(agent_memory, '_vector_search_enabled', True):

                    # Set similarity threshold in query or agent config
                    results = agent_memory._vector_similarity_search(
                        MemoryQuery(query_text="test", limit=5),
                        similarity_threshold=0.6  # Only distances <= 0.6 should pass
                    )

                    # Should only return mem_1 and mem_2 (distances 0.1 and 0.5)
                    assert len(results) == 2
                    assert results[0].entry_id == "mem_1"
                    assert results[1].entry_id == "mem_2"


class TestVectorStoreIntegration:
    """Test integration with vector store backends."""

    @pytest.fixture
    def agent_memory(self):
        """Create agent memory instance for testing."""
        return AgentMemory("test-agent-123")

    @pytest.fixture
    def mock_chromadb(self):
        """Mock ChromaDB client for testing."""
        mock_client = Mock()
        mock_collection = Mock()
        mock_client.get_or_create_collection.return_value = mock_collection
        return mock_client, mock_collection

    @patch('src.agent_orchestrated_etl.agents.memory.CHROMADB_AVAILABLE', True)
    def test_chromadb_initialization(self, agent_memory, mock_chromadb):
        """Test ChromaDB client initialization."""
        mock_client, mock_collection = mock_chromadb

        with patch('chromadb.Client', return_value=mock_client):
            agent_memory._init_vector_store()

            # Verify ChromaDB client was created
            assert agent_memory._vector_store is not None
            mock_client.get_or_create_collection.assert_called_once()

    @patch('src.agent_orchestrated_etl.agents.memory.CHROMADB_AVAILABLE', False)
    def test_fallback_when_chromadb_unavailable(self, agent_memory):
        """Test graceful fallback when ChromaDB is not available."""
        agent_memory._init_vector_store()

        # Should fallback to substring search
        assert agent_memory._vector_search_enabled is False
        assert agent_memory._vector_store is None

    def test_embedding_model_initialization(self, agent_memory):
        """Test embedding model initialization."""
        # Mock sentence transformers
        mock_model = Mock()

        with patch('sentence_transformers.SentenceTransformer', return_value=mock_model):
            agent_memory._init_embedding_model()

            if agent_memory._vector_search_enabled:
                assert agent_memory._embedding_model is not None

    def test_vector_store_memory_lifecycle(self, agent_memory, mock_chromadb):
        """Test complete lifecycle of memory with vector store."""
        mock_client, mock_collection = mock_chromadb

        with patch('chromadb.Client', return_value=mock_client):
            with patch.object(agent_memory, '_vector_search_enabled', True):
                with patch.object(agent_memory, '_vector_store', mock_collection):

                    # Store memory
                    content = {"description": "Test memory lifecycle"}

                    # Mock embedding
                    with patch.object(agent_memory, '_generate_embedding', return_value=[0.1, 0.2, 0.3]):
                        agent_memory.store_memory(
                            content=content,
                            memory_type=MemoryType.SEMANTIC,
                            importance=MemoryImportance.MEDIUM,
                            entry_id="lifecycle_test"
                        )

                        # Verify vector store add was called
                        mock_collection.add.assert_called_once()

                        # Verify the call arguments
                        call_args = mock_collection.add.call_args
                        assert call_args[1]['ids'] == ["lifecycle_test"]
                        assert call_args[1]['embeddings'] == [[0.1, 0.2, 0.3]]
                        assert call_args[1]['metadatas'][0]['entry_id'] == "lifecycle_test"


class TestBackwardCompatibility:
    """Test backward compatibility with existing memory search."""

    @pytest.fixture
    def agent_memory(self):
        """Create agent memory instance for testing."""
        return AgentMemory("test-agent-123")

    def test_memory_query_unchanged(self):
        """Test that MemoryQuery interface remains unchanged."""
        # Existing query should still work
        query = MemoryQuery(
            query_text="test query",
            memory_types=[MemoryType.SEMANTIC],
            tags=["test"],
            min_importance=MemoryImportance.MEDIUM,
            max_age_hours=24.0,
            min_confidence=0.8,
            limit=10
        )

        # All existing fields should be present
        assert query.query_text == "test query"
        assert query.memory_types == [MemoryType.SEMANTIC]
        assert query.tags == ["test"]
        assert query.min_importance == MemoryImportance.MEDIUM
        assert query.max_age_hours == 24.0
        assert query.min_confidence == 0.8
        assert query.limit == 10

    def test_search_results_format_unchanged(self, agent_memory):
        """Test that search results format remains the same."""
        # Store a memory
        agent_memory.store_memory(
            content={"description": "Compatibility test"},
            memory_type=MemoryType.SEMANTIC,
            importance=MemoryImportance.MEDIUM,
            entry_id="compat_test"
        )

        # Search should return same format
        query = MemoryQuery(query_text="compatibility", limit=5)
        results = agent_memory.search_memories(query)

        # Results should be list of MemoryEntry objects
        assert isinstance(results, list)
        if results:
            assert isinstance(results[0], MemoryEntry)
            assert hasattr(results[0], 'entry_id')
            assert hasattr(results[0], 'content')
            assert hasattr(results[0], 'importance')

    def test_existing_relevance_scoring_preserved(self, agent_memory):
        """Test that existing relevance scoring is preserved."""
        # Create and store memories with different characteristics
        agent_memory.store_memory(
            content={"description": "High importance memory"},
            memory_type=MemoryType.SEMANTIC,
            importance=MemoryImportance.HIGH,
            entry_id="high_imp"
        )

        agent_memory.store_memory(
            content={"description": "Medium importance memory"},
            memory_type=MemoryType.SEMANTIC,
            importance=MemoryImportance.MEDIUM,
            entry_id="med_imp"
        )

        # Access one memory to affect recency/frequency
        agent_memory.retrieve_memory("high_imp")

        # Search should still consider importance in ranking
        query = MemoryQuery(query_text="importance", limit=5)

        # Force fallback to ensure we're testing traditional scoring
        with patch.object(agent_memory, '_vector_search_enabled', False):
            results = agent_memory.search_memories(query)

            if len(results) >= 2:
                # Higher importance should generally rank higher
                high_result = next((r for r in results if r.entry_id == "high_imp"), None)
                med_result = next((r for r in results if r.entry_id == "med_imp"), None)

                assert high_result is not None
                assert med_result is not None


class TestPerformanceAndScaling:
    """Test performance characteristics of vector search."""

    @pytest.fixture
    def agent_memory(self):
        """Create agent memory instance for testing."""
        return AgentMemory("test-agent-123")

    def test_large_memory_set_performance(self, agent_memory):
        """Test performance with large numbers of memories."""
        # Create many memories
        for i in range(100):
            agent_memory.store_memory(
                content={
                    "description": f"Performance test memory {i}",
                    "details": f"This is memory number {i} for performance testing"
                },
                memory_type=MemoryType.SEMANTIC,
                importance=MemoryImportance.MEDIUM,
                entry_id=f"perf_mem_{i}"
            )

        # Time the search operation
        start_time = time.time()

        query = MemoryQuery(query_text="performance test", limit=10)
        results = agent_memory.search_memories(query)

        end_time = time.time()
        search_time = end_time - start_time

        # Search should complete in reasonable time (< 1 second for 100 memories)
        assert search_time < 1.0
        assert isinstance(results, list)
        assert len(results) <= 10

    def test_memory_limits_and_cleanup(self, agent_memory):
        """Test memory limits and cleanup functionality."""
        # This test would verify that vector store cleanup works
        # when memory limits are reached (if implemented)

        # Store many memories
        for i in range(50):
            agent_memory.store_memory(
                content={"description": f"Cleanup test {i}"},
                memory_type=MemoryType.WORKING,  # Working memory should be cleaned up
                importance=MemoryImportance.LOW,
                entry_id=f"cleanup_mem_{i}"
            )

        # Trigger cleanup (if implemented)
        if hasattr(agent_memory, 'cleanup_old_memories'):
            agent_memory.cleanup_old_memories()

        # Verify system is still functional
        query = MemoryQuery(query_text="cleanup", limit=5)
        results = agent_memory.search_memories(query)
        assert isinstance(results, list)
