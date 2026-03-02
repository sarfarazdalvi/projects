"""
Models package for Snowflake Performance Agent.
Contains data models and schemas used throughout the application.
"""

from .data_models import (
    AIConfig,
    QueryInfo,
    ProgressUpdate,
    ToolResult,
    Bottleneck,
    OptimizationRecommendation,
    InfrastructureChange,
    QueryEvaluationResult,
    QueryAnalysisResult,
    QueryProfile,
    PerformanceAnalysisData,
    SessionAnalysisSummary,
    AnalysisSummary,
    AggregatedRecommendations,
    OptimizationReport,
    WorkflowState
)

from .schemas import (
    OPERATOR_STATS_SCHEMA,
    QUERY_PERFORMANCE_SCHEMA,
    OPTIMIZATION_SCHEMA,
    SEMANTIC_EVALUATION_SCHEMA
)

__all__ = [
    # Data models
    'AIConfig',
    'QueryInfo',
    'ProgressUpdate',
    'ToolResult',
    'Bottleneck',
    'OptimizationRecommendation',
    'InfrastructureChange',
    'QueryEvaluationResult',
    'QueryAnalysisResult',
    'QueryProfile',
    'PerformanceAnalysisData',
    'SessionAnalysisSummary',
    'AnalysisSummary',
    'AggregatedRecommendations',
    'OptimizationReport',
    'WorkflowState',
    
    # Schemas
    'OPERATOR_STATS_SCHEMA',
    'QUERY_PERFORMANCE_SCHEMA',
    'OPTIMIZATION_SCHEMA',
    'SEMANTIC_EVALUATION_SCHEMA'
]