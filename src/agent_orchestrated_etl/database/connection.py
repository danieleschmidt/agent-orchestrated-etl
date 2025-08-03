"""Database connection management and configuration."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Union
from urllib.parse import urlparse

try:
    import asyncpg
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
    from sqlalchemy.orm import DeclarativeBase
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    asyncpg = None
    sa = None
    AsyncEngine = None
    async_sessionmaker = None
    create_async_engine = None
    DeclarativeBase = None
    SQLALCHEMY_AVAILABLE = False

from ..logging_config import get_logger
from ..exceptions import DatabaseException, ConfigurationError


class Base(DeclarativeBase if DeclarativeBase else object):
    """Base class for SQLAlchemy models."""
    pass


class DatabaseManager:
    """Manages database connections and operations."""
    
    def __init__(
        self,
        database_url: str,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        echo: bool = False,
    ):
        """Initialize database manager.
        
        Args:
            database_url: Database connection URL
            pool_size: Size of the connection pool
            max_overflow: Maximum overflow connections
            pool_timeout: Pool timeout in seconds
            echo: Whether to echo SQL statements
        """
        self.logger = get_logger("agent_etl.database")
        self.database_url = database_url
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.echo = echo
        
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker] = None
        self._connection_pool: Optional[asyncpg.Pool] = None
        self._is_initialized = False
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate database configuration."""
        if not self.database_url:
            raise ConfigurationError("Database URL is required")
        
        try:
            parsed = urlparse(self.database_url)
            if not parsed.scheme:
                raise ConfigurationError("Invalid database URL format")
        except Exception as e:
            raise ConfigurationError(f"Invalid database URL: {e}") from e
        
        if not SQLALCHEMY_AVAILABLE:
            self.logger.warning("SQLAlchemy not available, using mock database operations")
    
    async def initialize(self) -> None:
        """Initialize database connections and setup."""
        if self._is_initialized:
            return
        
        self.logger.info("Initializing database connections...")
        
        try:
            if SQLALCHEMY_AVAILABLE:
                await self._setup_sqlalchemy()
            await self._setup_asyncpg_pool()
            
            # Test connection
            await self._test_connection()
            
            self._is_initialized = True
            self.logger.info("Database connections initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise DatabaseException(f"Database initialization failed: {e}") from e
    
    async def _setup_sqlalchemy(self) -> None:
        """Setup SQLAlchemy async engine and session factory."""
        if not SQLALCHEMY_AVAILABLE:
            return
        
        # Convert sync URL to async if needed
        async_url = self.database_url
        if async_url.startswith("postgresql://"):
            async_url = async_url.replace("postgresql://", "postgresql+asyncpg://", 1)
        
        self._engine = create_async_engine(
            async_url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
            echo=self.echo,
            pool_pre_ping=True,
            pool_recycle=3600,  # Recycle connections every hour
        )
        
        self._session_factory = async_sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
        )
    
    async def _setup_asyncpg_pool(self) -> None:
        """Setup asyncpg connection pool for direct PostgreSQL access."""
        if not asyncpg:
            return
        
        try:
            # Parse connection parameters from URL
            parsed = urlparse(self.database_url)
            
            if parsed.scheme.startswith("postgresql"):
                self._connection_pool = await asyncpg.create_pool(
                    host=parsed.hostname,
                    port=parsed.port or 5432,
                    user=parsed.username,
                    password=parsed.password,
                    database=parsed.path[1:] if parsed.path else None,
                    min_size=2,
                    max_size=self.pool_size,
                    command_timeout=self.pool_timeout,
                    server_settings={
                        'application_name': 'agent-orchestrated-etl',
                        'jit': 'off'  # Disable JIT for better predictable performance
                    }
                )
        except Exception as e:
            self.logger.warning(f"Failed to setup asyncpg pool: {e}")
    
    async def _test_connection(self) -> None:
        """Test database connection."""
        try:
            if self._connection_pool:
                async with self._connection_pool.acquire() as conn:
                    await conn.fetchval('SELECT 1')
            elif SQLALCHEMY_AVAILABLE and self._engine:
                async with self._engine.begin() as conn:
                    await conn.execute(sa.text('SELECT 1'))
            else:
                self.logger.info("No database connection to test (mock mode)")
        except Exception as e:
            raise DatabaseException(f"Database connection test failed: {e}") from e
    
    async def close(self) -> None:
        """Close database connections."""
        self.logger.info("Closing database connections...")
        
        try:
            if self._connection_pool:
                await self._connection_pool.close()
                self._connection_pool = None
            
            if self._engine:
                await self._engine.dispose()
                self._engine = None
            
            self._session_factory = None
            self._is_initialized = False
            
            self.logger.info("Database connections closed")
            
        except Exception as e:
            self.logger.error(f"Error closing database connections: {e}")
    
    @contextlib.asynccontextmanager
    async def get_session(self) -> AsyncGenerator[Any, None]:
        """Get an async database session.
        
        Yields:
            Async session for database operations
        """
        if not self._is_initialized:
            await self.initialize()
        
        if not SQLALCHEMY_AVAILABLE or not self._session_factory:
            # Yield a mock session for testing
            yield MockSession()
            return
        
        async with self._session_factory() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                self.logger.error(f"Database session error: {e}")
                raise DatabaseException(f"Session error: {e}") from e
            finally:
                await session.close()
    
    @contextlib.asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        """Get a raw database connection.
        
        Yields:
            Raw asyncpg connection
        """
        if not self._is_initialized:
            await self.initialize()
        
        if not self._connection_pool:
            # Yield a mock connection for testing
            yield MockConnection()
            return
        
        async with self._connection_pool.acquire() as conn:
            try:
                yield conn
            except Exception as e:
                self.logger.error(f"Database connection error: {e}")
                raise DatabaseException(f"Connection error: {e}") from e
    
    async def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        fetch_mode: str = "all"
    ) -> Union[List[Dict[str, Any]], Dict[str, Any], None]:
        """Execute a query and return results.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters
            fetch_mode: "all", "one", or "none"
            
        Returns:
            Query results based on fetch_mode
        """
        if not self._is_initialized:
            await self.initialize()
        
        try:
            async with self.get_connection() as conn:
                if hasattr(conn, 'fetch'):  # asyncpg connection
                    if fetch_mode == "all":
                        rows = await conn.fetch(query, *(parameters.values() if parameters else []))
                        return [dict(row) for row in rows]
                    elif fetch_mode == "one":
                        row = await conn.fetchrow(query, *(parameters.values() if parameters else []))
                        return dict(row) if row else None
                    else:  # none
                        await conn.execute(query, *(parameters.values() if parameters else []))
                        return None
                else:  # Mock connection
                    return [] if fetch_mode == "all" else None
                    
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise DatabaseException(f"Query failed: {e}") from e
    
    async def execute_transaction(
        self,
        operations: List[Dict[str, Any]]
    ) -> List[Any]:
        """Execute multiple operations in a transaction.
        
        Args:
            operations: List of operation dicts with 'query' and optional 'parameters'
            
        Returns:
            List of results from each operation
        """
        if not self._is_initialized:
            await self.initialize()
        
        results = []
        
        try:
            async with self.get_connection() as conn:
                if hasattr(conn, 'transaction'):  # asyncpg connection
                    async with conn.transaction():
                        for op in operations:
                            query = op['query']
                            params = op.get('parameters', {})
                            fetch_mode = op.get('fetch_mode', 'none')
                            
                            if fetch_mode == "all":
                                rows = await conn.fetch(query, *(params.values() if params else []))
                                results.append([dict(row) for row in rows])
                            elif fetch_mode == "one":
                                row = await conn.fetchrow(query, *(params.values() if params else []))
                                results.append(dict(row) if row else None)
                            else:
                                await conn.execute(query, *(params.values() if params else []))
                                results.append(None)
                else:  # Mock connection
                    results = [None] * len(operations)
                    
        except Exception as e:
            self.logger.error(f"Transaction failed: {e}")
            raise DatabaseException(f"Transaction failed: {e}") from e
        
        return results
    
    async def get_health_status(self) -> Dict[str, Any]:
        """Get database health status.
        
        Returns:
            Health status information
        """
        try:
            start_time = time.time()
            
            # Test basic connectivity
            await self.execute_query("SELECT 1", fetch_mode="one")
            
            response_time = time.time() - start_time
            
            status = {
                "status": "healthy",
                "response_time_ms": round(response_time * 1000, 2),
                "pool_size": self.pool_size,
                "initialized": self._is_initialized,
                "sqlalchemy_available": SQLALCHEMY_AVAILABLE,
                "timestamp": time.time(),
            }
            
            # Add pool statistics if available
            if self._connection_pool and hasattr(self._connection_pool, 'get_size'):
                status.update({
                    "pool_current_size": self._connection_pool.get_size(),
                    "pool_max_size": self._connection_pool.get_max_size(),
                    "pool_min_size": self._connection_pool.get_min_size(),
                })
            
            return status
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": time.time(),
            }


class MockSession:
    """Mock session for testing when SQLAlchemy is not available."""
    
    async def commit(self):
        pass
    
    async def rollback(self):
        pass
    
    async def close(self):
        pass
    
    def add(self, obj):
        pass
    
    async def flush(self):
        pass
    
    async def refresh(self, obj):
        pass


class MockConnection:
    """Mock connection for testing when asyncpg is not available."""
    
    async def fetch(self, query, *args):
        return []
    
    async def fetchrow(self, query, *args):
        return None
    
    async def execute(self, query, *args):
        return None
    
    @contextlib.asynccontextmanager
    async def transaction(self):
        yield self


# Global database manager instance
_database_manager: Optional[DatabaseManager] = None


def get_database_manager() -> DatabaseManager:
    """Get the global database manager instance.
    
    Returns:
        DatabaseManager instance
        
    Raises:
        ConfigurationError: If database manager is not configured
    """
    global _database_manager
    
    if _database_manager is None:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise ConfigurationError("DATABASE_URL environment variable is required")
        
        _database_manager = DatabaseManager(
            database_url=database_url,
            pool_size=int(os.getenv("DATABASE_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("DATABASE_MAX_OVERFLOW", "20")),
            pool_timeout=int(os.getenv("DATABASE_POOL_TIMEOUT", "30")),
            echo=os.getenv("DATABASE_ECHO", "false").lower() == "true",
        )
    
    return _database_manager


async def initialize_database() -> None:
    """Initialize the global database manager."""
    db_manager = get_database_manager()
    await db_manager.initialize()


async def close_database() -> None:
    """Close the global database manager."""
    global _database_manager
    if _database_manager:
        await _database_manager.close()
        _database_manager = None