"""
Pydantic data models for Snowflake Performance Agent.

This module contains all the data models used throughout the application,
including configuration models, workflow state, and analysis results.
"""

import os
from typing import Optional, Dict, Any, List, TypedDict
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class AIConfig(BaseModel):
    """Configuration for Gemini AI client."""
    gemini_api_key: str = Field(description="Gemini API Key")
    model_name: str = Field(default="gemini-1.5-flash", description="Model identifier")
    temperature: float = Field(default=0.1, description="Temperature for AI model responses (0.0-1.0)")
    max_tokens: int = Field(default=8192, description="Maximum tokens in response")
    
    @classmethod
    def from_env(cls) -> 'AIConfig':
        """Create AI configuration from environment variables."""
        gemini_api_key = os.environ.get('GEMINI_API_KEY')
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set")
        
        return cls(
            gemini_api_key=gemini_api_key,
            model_name='gemini-1.5-flash'
        )


class QueryInfo(BaseModel):
    """Information about a query from history."""
    query_id: str
    query_text: str
    execution_stats: Dict[str, Any]
    session_id: Optional[str] = None


class ProgressUpdate(BaseModel):
    """Progress update for real-time tracking."""
    tool_name: str
    status: str  # "starting", "running", "completed", "failed"
    progress_percentage: float
    message: str
    execution_time_ms: Optional[float] = None


class ToolResult(BaseModel):
    """Result from tool execution."""
    success: bool
    tool_name: str
    data: Any
    error_message: Optional[str] = None
    execution_time_ms: float = 0.0


class Bottleneck(BaseModel):
    """Represents a performance bottleneck identified in query analysis."""
    type: str = Field(description="Type of bottleneck (e.g., 'table_scan', 'join_operation', 'sorting')")
    description: str = Field(description="Human-readable description of the bottleneck")
    severity: str = Field(description="Severity level (flexible: any descriptive string like 'low', 'medium', 'high', 'critical', 'medium-high', etc.)")
    impact: str = Field(description="Description of performance impact")
    root_cause: Optional[str] = Field(default=None, description="Root cause analysis of the performance issue")
    affected_tables: List[str] = Field(default_factory=list, description="List of tables affected by this bottleneck")
    resource_impact: Dict[str, str] = Field(default_factory=dict, description="Resource impact breakdown (cpu, io, memory)")


class OptimizationRecommendation(BaseModel):
    """Represents an optimization recommendation for improving query performance."""
    type: str = Field(description="Type of optimization (e.g., 'index', 'clustering', 'partitioning')")
    description: str = Field(description="Detailed description of the recommendation")
    expected_improvement: str = Field(description="Expected performance improvement")
    ddl_suggestion: Optional[str] = Field(default=None, description="DDL statement to implement the optimization")
    priority: str = Field(default="medium", description="Priority level: 'low', 'medium', 'high', 'critical'")
    
    @field_validator('priority')
    @classmethod
    def validate_priority(cls, v):
        allowed = {'low', 'medium', 'high', 'critical'}
        if v not in allowed:
            raise ValueError(f'Priority must be one of {allowed}')
        return v


class InfrastructureChange(BaseModel):
    """Represents an infrastructure-level change recommendation."""
    type: str = Field(description="Type of infrastructure change")
    recommendation: str = Field(description="Specific recommendation")
    justification: str = Field(description="Justification for the change")
    estimated_cost_impact: Optional[str] = Field(default=None, description="Estimated cost impact")


class QueryEvaluationResult(BaseModel):
    """Result of semantic evaluation between original and optimized query."""
    query_id: str = Field(description="Query identifier")
    semantic_equivalence: bool = Field(description="Whether queries are semantically equivalent")
    confidence_score: float = Field(description="Confidence score (0-1) for the evaluation")
    differences_found: List[str] = Field(default_factory=list, description="List of differences identified")
    recommendation: str = Field(description="Recommendation based on evaluation")
    feedback_for_optimization: Optional[str] = Field(default=None, description="Feedback to provide back to optimization step")
    evaluation_timestamp: str = Field(description="ISO timestamp of evaluation")


class QueryAnalysisResult(BaseModel):
    """Complete analysis result for a single query."""
    query_id: str = Field(description="Unique identifier for the query")
    query_hash: str = Field(description="Hash of the query text for identification")
    original_query_text: str = Field(description="Original SQL query text")
    execution_time_seconds: float = Field(description="Query execution time in seconds")
    bottlenecks: List[Bottleneck] = Field(default_factory=list, description="Identified bottlenecks")
    optimization_recommendations: List[OptimizationRecommendation] = Field(
        default_factory=list,
        description="Optimization recommendations"
    )
    query_rewrite_needed: bool = Field(default=False, description="Whether query rewrite is needed")
    optimized_query: Optional[str] = Field(default=None, description="Optimized version of the query")
    infrastructure_changes: List[InfrastructureChange] = Field(
        default_factory=list,
        description="Infrastructure changes recommended"
    )
    estimated_performance_gain: str = Field(description="Estimated performance improvement")
    analysis_timestamp: str = Field(description="ISO timestamp of when analysis was performed")


class QueryProfile(BaseModel):
    """Detailed profiling information for a query."""
    query_id: str = Field(description="Query identifier")
    operator_stats: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Snowflake operator statistics"
    )
    execution_plan: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Query execution plan details"
    )
    resource_usage: Dict[str, float] = Field(
        default_factory=dict,
        description="Resource usage metrics"
    )


class PerformanceAnalysisData(BaseModel):
    """Container for all performance analysis data."""
    analyses: List[QueryAnalysisResult] = Field(
        default_factory=list,
        description="Individual query analysis results"
    )
    query_count: int = Field(description="Total number of queries analyzed")
    analysis_timestamp: str = Field(description="When the analysis was completed")


class SessionAnalysisSummary(BaseModel):
    """Summary of session-level analysis."""
    session_id: str = Field(description="Snowflake session identifier")
    analysis_timestamp: str = Field(description="ISO timestamp of analysis")
    total_queries_analyzed: int = Field(description="Total number of queries analyzed")
    total_execution_time_seconds: float = Field(description="Total execution time across all queries")


class AnalysisSummary(BaseModel):
    """High-level summary of analysis findings."""
    total_bottlenecks_found: int = Field(description="Total number of bottlenecks identified")
    queries_needing_rewrite: int = Field(description="Number of queries that need rewriting")
    infrastructure_changes_recommended: int = Field(description="Number of infrastructure changes recommended")
    estimated_total_improvement: str = Field(description="Overall estimated improvement")


class AggregatedRecommendations(BaseModel):
    """Aggregated recommendations across all analyzed queries."""
    top_bottlenecks: List[Bottleneck] = Field(
        default_factory=list,
        description="Top bottlenecks found across all queries"
    )
    priority_optimizations: List[OptimizationRecommendation] = Field(
        default_factory=list,
        description="Priority optimization recommendations"
    )
    infrastructure_changes: List[InfrastructureChange] = Field(
        default_factory=list,
        description="All infrastructure change recommendations"
    )


class OptimizationReport(BaseModel):
    """Complete optimization report for a session."""
    session_analysis: SessionAnalysisSummary = Field(description="Session analysis summary")
    summary: AnalysisSummary = Field(description="High-level analysis summary")
    detailed_analyses: List[QueryAnalysisResult] = Field(description="Detailed per-query analyses")
    aggregated_recommendations: AggregatedRecommendations = Field(description="Aggregated recommendations")
    next_steps: List[str] = Field(description="Recommended next steps")


class WorkflowState(TypedDict):
    """State for the LangGraph Performance Analysis workflow."""
    # Input parameters - either session_id OR query_tag+start_date OR query_id
    session_id: Optional[str]
    query_tag: Optional[str]
    start_date: Optional[str]
    query_id: Optional[str]
    
    # Configuration 
    ai_config: Dict[str, Any]
    
    # Workflow control
    messages: List[str]
    current_step: str
    
    # Data collected by tools
    query_history: List[Dict[str, Any]]  # Serialized QueryInfo objects
    query_profiles: Dict[str, Dict[str, Any]]  # Serialized QueryProfile objects
    bottleneck_analyses: Dict[str, Any]
    table_object_details: Dict[str, Any]
    performance_analyses: Dict[str, Any]
    performance_analysis: Optional[Dict[str, Any]]  # Serialized PerformanceAnalysisData
    optimization_report: Optional[Dict[str, Any]]  # Serialized OptimizationReport
    
    # Evaluation and feedback loop tracking
    query_evaluations: Dict[str, Dict[str, Any]]  # Serialized QueryEvaluationResult objects
    optimization_retry_count: Dict[str, int]
    optimization_feedback: Dict[str, List[str]]
    max_optimization_retries: int
    
    # Selective optimization tracking
    queries_needing_reoptimization: List[str]  # Using List instead of Set for JSON serialization
    
    # Tool results history
    tool_results: List[Dict[str, Any]]  # Serialized ToolResult objects
    
    # Status and error tracking
    is_complete: bool
    errors: List[str]