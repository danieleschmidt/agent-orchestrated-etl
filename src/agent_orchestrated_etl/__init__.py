"""Agent-Orchestrated ETL package."""

# Core imports that don't require external dependencies
from . import (
    cli,
    config,
    core,
    dag_generator,
    data_source_analysis,
    orchestrator,
)

# Basic classes that are always available
from .orchestrator import DataOrchestrator, MonitorAgent, Pipeline

# Optional imports with graceful fallback
try:
    from . import performance_optimizer
    from .performance_optimizer import PerformanceOptimizer
    _PERFORMANCE_AVAILABLE = True
except ImportError:
    performance_optimizer = None
    PerformanceOptimizer = None
    _PERFORMANCE_AVAILABLE = False

try:
    from . import production_deployment
    from .production_deployment import ProductionDeploymentManager
    _PRODUCTION_AVAILABLE = True
except ImportError:
    production_deployment = None
    ProductionDeploymentManager = None
    _PRODUCTION_AVAILABLE = False

try:
    from . import quantum_planner, adaptive_resources
    from .quantum_planner import QuantumPipelineOrchestrator, QuantumTaskPlanner
    from .adaptive_resources import AdaptiveResourceManager
    _QUANTUM_AVAILABLE = True
    _ADAPTIVE_AVAILABLE = True
except ImportError:
    quantum_planner = None
    adaptive_resources = None
    QuantumPipelineOrchestrator = None
    QuantumTaskPlanner = None
    AdaptiveResourceManager = None
    _QUANTUM_AVAILABLE = False
    _ADAPTIVE_AVAILABLE = False

try:
    from . import enhanced_orchestrator
    from .enhanced_orchestrator import (
        DataPipeline,
        EnhancedDataOrchestrator,
        PipelineConfig,
    )
    _ENHANCED_AVAILABLE = True
except ImportError:
    enhanced_orchestrator = None
    DataPipeline = None
    EnhancedDataOrchestrator = None
    PipelineConfig = None
    _ENHANCED_AVAILABLE = False

# Build __all__ list based on what's available
__all__ = [
    "config",
    "core",
    "data_source_analysis",
    "dag_generator",
    "cli",
    "orchestrator",
    "DataOrchestrator",
    "Pipeline",
    "MonitorAgent",
]

# Add optional components to __all__ if available
if _PERFORMANCE_AVAILABLE:
    __all__.extend(["performance_optimizer", "PerformanceOptimizer"])

if _PRODUCTION_AVAILABLE:
    __all__.extend(["production_deployment", "ProductionDeploymentManager"])

if _QUANTUM_AVAILABLE:
    __all__.extend(["quantum_planner", "QuantumTaskPlanner", "QuantumPipelineOrchestrator"])

if _ADAPTIVE_AVAILABLE:
    __all__.extend(["adaptive_resources", "AdaptiveResourceManager"])

if _ENHANCED_AVAILABLE:
    __all__.extend([
        "enhanced_orchestrator",
        "DataPipeline",
        "EnhancedDataOrchestrator", 
        "PipelineConfig"
    ])
