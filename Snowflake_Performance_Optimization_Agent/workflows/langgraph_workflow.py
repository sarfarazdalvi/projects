"""
LangGraph workflow orchestration for Snowflake Performance Analysis.

This module contains the workflow nodes, conditional logic, and the main
SnowflakePerformanceLangGraphAgent class that orchestrates the entire analysis process.
"""

import logging
from typing import Optional, Callable, Dict, Any
from datetime import datetime

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from models.data_models import (
    AIConfig,
    WorkflowState,
    ProgressUpdate,
    QueryProfile,
    QueryAnalysisResult,
    QueryEvaluationResult
)
from tools.snowflake_tools import (
    QueryHistoryTool,
    QueryProfilingTool,
    QueryObjectDetailsTool,
    ReportGenerationTool
)
from tools.ai_tools import (
    OperatorStatsAnalysisTool,
    QueryPerformanceAnalysisTool,
    OptimizedQueryGenerationTool,
    QuerySemanticEvaluationTool
)

logger = logging.getLogger(__name__)


def _send_progress_update(state: WorkflowState, tool_name: str, status: str, progress_percentage: float, message: str, execution_time_ms: Optional[float] = None):
    """Send progress update to callback if available."""
    # Access progress callback from class variable
    if SnowflakePerformanceLangGraphAgent._current_progress_callback:
        try:
            update = ProgressUpdate(
                tool_name=tool_name,
                status=status,
                progress_percentage=progress_percentage,
                message=message,
                execution_time_ms=execution_time_ms
            )
            SnowflakePerformanceLangGraphAgent._current_progress_callback(update)
        except Exception as e:
            logger.warning(f"⚠️ Progress callback failed: {e}")


def query_history_node(state: WorkflowState) -> WorkflowState:
    """Node to fetch query history using generic connection."""
    logger.info("📊 Executing query_history_node")
    state["current_step"] = "query_history"
    
    session_id = state.get("session_id")
    query_tag = state.get("query_tag")
    start_date = state.get("start_date")
    query_id = state.get("query_id")
    
    if session_id:
        _send_progress_update(state, "query_history", "starting", 0.0, f"Fetching slow queries from session {session_id}")
    elif query_tag and start_date:
        _send_progress_update(state, "query_history", "starting", 0.0, f"Searching for queries with tag '{query_tag}' from {start_date}")
    elif query_id:
        _send_progress_update(state, "query_history", "starting", 0.0, f"Fetching specific query {query_id}")
    
    # Initialize tool
    history_tool = QueryHistoryTool()
    
    # Execute tool
    result = history_tool(session_id=session_id, query_tag=query_tag, start_date=start_date, query_id=query_id, limit=5)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        queries_data = result.data.get('queries', [])
        state["query_history"] = queries_data
        state["messages"].append(f"✅ Found {len(queries_data)} slow queries")
        _send_progress_update(state, "query_history", "completed", 100.0, f"✅ Found {len(queries_data)} slow queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Query history failed: {result.error_message}")
        _send_progress_update(state, "query_history", "failed", 100.0, f"❌ Query history failed: {result.error_message}", result.execution_time_ms)
    
    return state


def query_profiling_node(state: WorkflowState) -> WorkflowState:
    """Node to profile queries using generic connection."""
    logger.info("🔍 Executing query_profiling_node")
    state["current_step"] = "query_profiling"
    
    query_ids = [q["query_id"] for q in state["query_history"]]
    _send_progress_update(state, "query_profiling", "starting", 0.0, f"Profiling {len(query_ids)} queries")
    
    # Initialize tool
    profiling_tool = QueryProfilingTool()
    
    # Execute tool
    result = profiling_tool(query_ids)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        profiles_data = result.data.get('profiles', {})
        state["query_profiles"] = profiles_data
        state["messages"].append(f"✅ Profiled {result.data.get('query_count', 0)} queries")
        _send_progress_update(state, "query_profiling", "completed", 100.0, f"✅ Profiled {result.data.get('query_count', 0)} queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Query profiling failed: {result.error_message}")
        _send_progress_update(state, "query_profiling", "failed", 100.0, f"❌ Query profiling failed: {result.error_message}", result.execution_time_ms)
    
    return state


def query_object_details_node(state: WorkflowState) -> WorkflowState:
    """Node to fetch table object details using generic connection."""
    logger.info("📊 Executing query_object_details_node")
    state["current_step"] = "query_object_details"
    
    query_ids = [q["query_id"] for q in state["query_history"]]
    _send_progress_update(state, "query_object_details", "starting", 0.0, f"Fetching table details for {len(query_ids)} queries")
    
    # Initialize tool
    object_details_tool = QueryObjectDetailsTool()
    
    # Execute tool
    result = object_details_tool(query_ids)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        state["table_object_details"] = result.data.get('table_details', {})
        state["messages"].append(f"✅ Table object details collected for {result.data.get('query_count', 0)} queries")
        _send_progress_update(state, "query_object_details", "completed", 100.0, f"✅ Table object details collected for {result.data.get('query_count', 0)} queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Table object details collection failed: {result.error_message}")
        _send_progress_update(state, "query_object_details", "failed", 100.0, f"❌ Table object details collection failed: {result.error_message}", result.execution_time_ms)
    
    return state


def operator_stats_analysis_node(state: WorkflowState) -> WorkflowState:
    """Node to analyze operator statistics."""
    logger.info("🔍 Executing operator_stats_analysis_node")
    state["current_step"] = "operator_stats_analysis"
    
    _send_progress_update(state, "operator_stats_analysis", "starting", 0.0, "Analyzing operator statistics")
    
    # Initialize tool with AI config
    ai_config = AIConfig(**state["ai_config"])
    operator_analysis_tool = OperatorStatsAnalysisTool(ai_config)
    
    # Convert profiles to QueryProfile objects
    query_profiles = {}
    for query_id, profile_data in state["query_profiles"].items():
        query_profiles[query_id] = QueryProfile(**profile_data)
    
    # Execute tool
    result = operator_analysis_tool(query_profiles)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        state["bottleneck_analyses"] = result.data.get('bottleneck_analyses', {})
        state["messages"].append(f"✅ Operator stats analysis completed for {result.data.get('query_count', 0)} queries")
        _send_progress_update(state, "operator_stats_analysis", "completed", 100.0, f"✅ Operator stats analysis completed for {result.data.get('query_count', 0)} queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Operator stats analysis failed: {result.error_message}")
        _send_progress_update(state, "operator_stats_analysis", "failed", 100.0, f"❌ Operator stats analysis failed: {result.error_message}", result.execution_time_ms)
    
    return state


def query_performance_analysis_node(state: WorkflowState) -> WorkflowState:
    """Node to perform query performance analysis."""
    logger.info("📈 Executing query_performance_analysis_node")
    state["current_step"] = "query_performance_analysis"
    
    _send_progress_update(state, "query_performance_analysis", "starting", 0.0, "Analyzing query performance characteristics")
    
    # Initialize tool with AI config
    ai_config = AIConfig(**state["ai_config"])
    performance_analysis_tool = QueryPerformanceAnalysisTool(ai_config)
    
    # Convert profiles to QueryProfile objects
    query_profiles = {}
    for query_id, profile_data in state["query_profiles"].items():
        query_profiles[query_id] = QueryProfile(**profile_data)
    
    # Execute tool
    queries = state["query_history"]
    bottleneck_analyses = state["bottleneck_analyses"] if state["bottleneck_analyses"] else None
    result = performance_analysis_tool(queries, query_profiles, bottleneck_analyses)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        state["performance_analyses"] = result.data.get('performance_analyses', {})
        state["messages"].append(f"✅ Query performance analysis completed for {result.data.get('query_count', 0)} queries")
        _send_progress_update(state, "query_performance_analysis", "completed", 100.0, f"✅ Query performance analysis completed for {result.data.get('query_count', 0)} queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Query performance analysis failed: {result.error_message}")
        _send_progress_update(state, "query_performance_analysis", "failed", 100.0, f"❌ Query performance analysis failed: {result.error_message}", result.execution_time_ms)
    
    return state


def optimized_query_generation_node(state: WorkflowState) -> WorkflowState:
    """Node to generate optimized queries."""
    logger.info("🔧 Executing optimized_query_generation_node")
    state["current_step"] = "optimized_query_generation"
    
    # Check if this is a retry scenario
    queries_needing_reoptimization = set(state.get("queries_needing_reoptimization", []))
    
    if queries_needing_reoptimization:
        # Only reprocess failing queries on retry
        queries_to_process = [
            q for q in state["query_history"]
            if q["query_id"] in queries_needing_reoptimization
        ]
        _send_progress_update(state, "optimized_query_generation", "starting", 0.0, f"Re-optimizing {len(queries_to_process)} failed queries")
    else:
        # First optimization run - process all queries
        queries_to_process = state["query_history"]
        _send_progress_update(state, "optimized_query_generation", "starting", 0.0, f"Generating optimized queries for {len(queries_to_process)} queries")
    
    # Initialize tool with AI config
    ai_config = AIConfig(**state["ai_config"])
    optimization_tool = OptimizedQueryGenerationTool(ai_config)
    
    # Execute tool
    performance_analyses = state["performance_analyses"] if state["performance_analyses"] else {}
    previous_feedback = state["optimization_feedback"] if state["optimization_feedback"] else None
    table_details = state["table_object_details"] if state["table_object_details"] else None
    
    result = optimization_tool(queries_to_process, performance_analyses, previous_feedback, table_details)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        new_analysis_data = result.data
        
        # Handle merging with existing optimization results
        if state["performance_analysis"] is None:
            # First optimization run - store all results
            state["performance_analysis"] = new_analysis_data
        else:
            # Selective re-optimization - merge with existing results while preserving ALL queries
            existing_analyses = {analysis["query_id"]: analysis for analysis in state["performance_analysis"]["analyses"]}
            reprocessed_query_ids = set()
            
            # Update with new optimization results
            for new_analysis in new_analysis_data["analyses"]:
                existing_analyses[new_analysis["query_id"]] = new_analysis
                reprocessed_query_ids.add(new_analysis["query_id"])
            
            # Create merged performance analysis data
            state["performance_analysis"] = {
                "analyses": list(existing_analyses.values()),
                "query_count": len(existing_analyses),
                "analysis_timestamp": datetime.now().isoformat()
            }
        
        # Clear the re-optimization set after successful processing
        state["queries_needing_reoptimization"] = []
        
        state["messages"].append(f"✅ Optimized query generation completed for {result.data.get('query_count', 0)} queries")
        _send_progress_update(state, "optimized_query_generation", "completed", 100.0, f"✅ Optimized query generation completed for {result.data.get('query_count', 0)} queries", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Optimized query generation failed: {result.error_message}")
        # Clear the re-optimization set to prevent infinite loops
        state["queries_needing_reoptimization"] = []
        _send_progress_update(state, "optimized_query_generation", "failed", 100.0, f"❌ Optimized query generation failed: {result.error_message}", result.execution_time_ms)
    
    return state


def query_semantic_evaluation_node(state: WorkflowState) -> WorkflowState:
    """Node to evaluate semantic equivalence of optimized queries."""
    logger.info("🔍 Executing query_semantic_evaluation_node")
    state["current_step"] = "query_semantic_evaluation"
    
    _send_progress_update(state, "query_semantic_evaluation", "starting", 0.0, "Evaluating semantic equivalence")
    
    # Initialize tool with AI config
    ai_config = AIConfig(**state["ai_config"])
    evaluation_tool = QuerySemanticEvaluationTool(ai_config)
    
    # Convert analyses to QueryAnalysisResult objects
    analyses = [QueryAnalysisResult(**analysis) for analysis in state["performance_analysis"]["analyses"]]
    previous_feedback = state["optimization_feedback"] if state["optimization_feedback"] else None
    
    # Execute tool
    result = evaluation_tool(analyses, previous_feedback)
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        # Process evaluation results and implement feedback loop
        evaluations_data = result.data.get('evaluations', {})
        
        # Convert to and store evaluation results
        for query_id, eval_data in evaluations_data.items():
            state["query_evaluations"][query_id] = eval_data
            
            # Handle feedback and retry logic
            evaluation = QueryEvaluationResult(**eval_data)
            if evaluation.recommendation in ["REJECT", "RETRY_WITH_FEEDBACK"]:
                # Initialize retry count if not exists
                if query_id not in state["optimization_retry_count"]:
                    state["optimization_retry_count"][query_id] = 0
                
                # Check if we can retry (haven't exceeded max retries)
                if state["optimization_retry_count"][query_id] < state["max_optimization_retries"]:
                    # Increment retry count
                    state["optimization_retry_count"][query_id] += 1
                    
                    # Store feedback for next optimization attempt
                    if query_id not in state["optimization_feedback"]:
                        state["optimization_feedback"][query_id] = []
                    
                    if evaluation.feedback_for_optimization:
                        state["optimization_feedback"][query_id].append(
                            f"Attempt {state['optimization_retry_count'][query_id]}: {evaluation.feedback_for_optimization}"
                        )
                    
                    # Add query to re-optimization list for selective processing
                    if query_id not in state["queries_needing_reoptimization"]:
                        state["queries_needing_reoptimization"].append(query_id)
        
        summary = result.data.get('summary', {})
        total_evaluations = summary.get('total_evaluations', 0)
        accepted_queries = summary.get('accepted_queries', 0)
        retry_queries = summary.get('retry_queries', 0)
        
        state["messages"].append(f"✅ Query semantic evaluation completed: {accepted_queries}/{total_evaluations} queries accepted, {retry_queries} need retry")
        _send_progress_update(state, "query_semantic_evaluation", "completed", 100.0, f"✅ Query semantic evaluation completed: {accepted_queries}/{total_evaluations} queries accepted, {retry_queries} need retry", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Query semantic evaluation failed: {result.error_message}")
        _send_progress_update(state, "query_semantic_evaluation", "failed", 100.0, f"❌ Query semantic evaluation failed: {result.error_message}", result.execution_time_ms)
    
    return state


def report_generation_node(state: WorkflowState) -> WorkflowState:
    """Node to generate the final optimization report."""
    logger.info("📋 Executing report_generation_node")
    state["current_step"] = "report_generation"
    
    _send_progress_update(state, "report_generation", "starting", 0.0, "Generating final analysis report")
    
    # Initialize tool
    report_tool = ReportGenerationTool()
    
    # Convert analyses to QueryAnalysisResult objects
    analyses = []
    if state["performance_analysis"]:
        analyses = [QueryAnalysisResult(**analysis) for analysis in state["performance_analysis"]["analyses"]]
    
    # Execute tool
    result = report_tool(
        session_id=state.get("session_id"),
        analyses=analyses,
        query_tag=state.get("query_tag"),
        start_date=state.get("start_date"),
        query_id=state.get("query_id")
    )
    
    # Update state
    state["tool_results"].append(result.model_dump())
    
    if result.success:
        state["optimization_report"] = result.data
        state["is_complete"] = True
        state["messages"].append("✅ Optimization report generated successfully")
        _send_progress_update(state, "report_generation", "completed", 100.0, "✅ Optimization report generated successfully", result.execution_time_ms)
    else:
        state["errors"].append(f"❌ Report generation failed: {result.error_message}")
        _send_progress_update(state, "report_generation", "failed", 100.0, f"❌ Report generation failed: {result.error_message}", result.execution_time_ms)
    
    return state


# Conditional logic functions
def should_continue_after_query_history(state: WorkflowState) -> str:
    """Determine next step after fetching query history."""
    if len(state.get("query_history", [])) > 0:
        return "query_profiling"
    else:
        return END


def should_continue_after_profiling(state: WorkflowState) -> str:
    """Determine next step after query profiling."""
    query_ids_from_history = {q["query_id"] for q in state.get("query_history", [])}
    profiled_query_ids = set(state.get("query_profiles", {}).keys())
    
    if query_ids_from_history.issubset(profiled_query_ids):
        return "query_object_details"
    else:
        return END


def should_continue_after_object_details(state: WorkflowState) -> str:
    """Determine next step after fetching table object details."""
    query_ids_from_history = {q["query_id"] for q in state.get("query_history", [])}
    table_details_query_ids = set(state.get("table_object_details", {}).keys())
    
    if query_ids_from_history.issubset(table_details_query_ids):
        return "operator_stats_analysis"
    else:
        return END


def should_continue_after_operator_analysis(state: WorkflowState) -> str:
    """Determine next step after operator statistics analysis."""
    query_ids_from_history = {q["query_id"] for q in state.get("query_history", [])}
    bottleneck_analysis_query_ids = set(state.get("bottleneck_analyses", {}).keys())
    
    if query_ids_from_history.issubset(bottleneck_analysis_query_ids):
        return "query_performance_analysis"
    else:
        return END


def should_continue_after_performance_analysis(state: WorkflowState) -> str:
    """Determine next step after performance analysis."""
    query_ids_from_history = {q["query_id"] for q in state.get("query_history", [])}
    performance_analysis_query_ids = set(state.get("performance_analyses", {}).keys())
    
    if query_ids_from_history.issubset(performance_analysis_query_ids):
        return "optimized_query_generation"
    else:
        return END


def should_continue_after_optimization(state: WorkflowState) -> str:
    """Determine next step after optimization generation."""
    # Check if there are queries needing re-optimization
    queries_needing_reoptimization = state.get("queries_needing_reoptimization", [])
    if len(queries_needing_reoptimization) > 0:
        # Check retry counts to prevent infinite loops
        retryable_queries = []
        for query_id in queries_needing_reoptimization:
            retry_count = state.get("optimization_retry_count", {}).get(query_id, 0)
            if retry_count < state.get("max_optimization_retries", 3):
                retryable_queries.append(query_id)
        
        if retryable_queries:
            return "optimized_query_generation"  # Retry optimization
    
    if state.get("performance_analysis"):
        return "query_semantic_evaluation"
    else:
        return END


def should_continue_after_evaluation(state: WorkflowState) -> str:
    """Determine next step after semantic evaluation."""
    # Check if there are new queries needing re-optimization after evaluation
    queries_needing_reoptimization = state.get("queries_needing_reoptimization", [])
    if len(queries_needing_reoptimization) > 0:
        # Check retry counts to prevent infinite loops
        retryable_queries = []
        for query_id in queries_needing_reoptimization:
            retry_count = state.get("optimization_retry_count", {}).get(query_id, 0)
            if retry_count < state.get("max_optimization_retries", 3):
                retryable_queries.append(query_id)
        
        if retryable_queries:
            return "optimized_query_generation"  # Retry optimization with feedback
    
    # All evaluations complete, proceed to report generation
    return "report_generation"


def should_end_after_report(state: WorkflowState) -> str:
    """Determine if workflow should end after report generation."""
    if state.get("is_complete"):
        return END
    else:
        return END  # End even if not successful to prevent infinite loops


class SnowflakePerformanceLangGraphAgent:
    """
    LangGraph Agent for Snowflake Performance Analysis.
    
    This agent uses deterministic workflow orchestration:
    - Uses LangGraph StateGraph for workflow management
    - Eliminates AI-driven reasoning to reduce LLM calls
    - Maintains strict sequential execution of analysis steps
    - Uses conditional edges for deterministic state transitions
    - Uses generic connection for direct Snowflake access
    """
    
    # Class variable to store the current instance's progress callback
    _current_progress_callback: Optional[Callable[[ProgressUpdate], None]] = None
    
    def __init__(self, ai_config: AIConfig, progress_callback: Optional[Callable[[ProgressUpdate], None]] = None):
        # Only AI config needed, no Snowflake config
        self.ai_config = ai_config
        self.progress_callback = progress_callback
        
        # Build the LangGraph workflow
        self.workflow = self._build_workflow()
        
        logger.info("🚀 SnowflakePerformanceLangGraphAgent initialized with deterministic workflow orchestration")
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph StateGraph workflow."""
        # Create the state graph
        workflow = StateGraph(WorkflowState)
        
        # Add all workflow nodes
        workflow.add_node("query_history", query_history_node)
        workflow.add_node("query_profiling", query_profiling_node)
        workflow.add_node("query_object_details", query_object_details_node)
        workflow.add_node("operator_stats_analysis", operator_stats_analysis_node)
        workflow.add_node("query_performance_analysis", query_performance_analysis_node)
        workflow.add_node("optimized_query_generation", optimized_query_generation_node)
        workflow.add_node("query_semantic_evaluation", query_semantic_evaluation_node)
        workflow.add_node("report_generation", report_generation_node)
        
        # Set entry point directly to query history
        workflow.set_entry_point("query_history")
        
        # Add conditional edges for deterministic workflow control
        workflow.add_conditional_edges(
            "query_history",
            should_continue_after_query_history,
            {
                "query_profiling": "query_profiling",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "query_profiling",
            should_continue_after_profiling,
            {
                "query_object_details": "query_object_details",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "query_object_details",
            should_continue_after_object_details,
            {
                "operator_stats_analysis": "operator_stats_analysis",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "operator_stats_analysis",
            should_continue_after_operator_analysis,
            {
                "query_performance_analysis": "query_performance_analysis",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "query_performance_analysis",
            should_continue_after_performance_analysis,
            {
                "optimized_query_generation": "optimized_query_generation",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "optimized_query_generation",
            should_continue_after_optimization,
            {
                "optimized_query_generation": "optimized_query_generation",  # For retries
                "query_semantic_evaluation": "query_semantic_evaluation",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "query_semantic_evaluation",
            should_continue_after_evaluation,
            {
                "optimized_query_generation": "optimized_query_generation",  # For retries with feedback
                "report_generation": "report_generation",
                END: END
            }
        )
        
        workflow.add_conditional_edges(
            "report_generation",
            should_end_after_report,
            {
                END: END
            }
        )
        
        return workflow.compile(checkpointer=MemorySaver())
    
    def analyze_session_performance(self, session_id: str = None, query_tag: str = None, start_date: str = None, query_id: str = None, progress_callback: Optional[Callable[[ProgressUpdate], None]] = None) -> Dict[str, Any]:
        """
        Main method to analyze performance using LangGraph deterministic workflow.
        
        Args:
            session_id: Snowflake session ID to analyze (alternative 1)
            query_tag: Query tag to filter queries (alternative 2 - requires start_date)
            start_date: Start date to filter queries from (alternative 2 - requires query_tag)
            query_id: Specific query ID to analyze (alternative 3)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Optimization report or error information
        """
        # Validate input parameters
        if session_id:
            logger.info(f"🚀 Starting LangGraph analysis for session: {session_id}")
        elif query_tag and start_date:
            logger.info(f"🚀 Starting LangGraph analysis for query tag '{query_tag}' from {start_date}")
        elif query_id:
            logger.info(f"🚀 Starting LangGraph analysis for query: {query_id}")
        else:
            raise ValueError("Either session_id, query_id, or both query_tag and start_date must be provided")
            
        logger.info("=" * 80)
        
        # Set up progress callback if provided
        if progress_callback:
            self.progress_callback = progress_callback
        
        # Set the current progress callback for the workflow execution
        SnowflakePerformanceLangGraphAgent._current_progress_callback = self.progress_callback or progress_callback
        
        # Initialize state
        initial_state = WorkflowState(
            session_id=session_id,
            query_tag=query_tag,
            start_date=start_date,
            query_id=query_id,
            ai_config=self.ai_config.model_dump(),
            messages=[],
            current_step="",
            query_history=[],
            query_profiles={},
            bottleneck_analyses={},
            table_object_details={},
            performance_analyses={},
            performance_analysis=None,
            optimization_report=None,
            query_evaluations={},
            optimization_retry_count={},
            optimization_feedback={},
            max_optimization_retries=3,
            queries_needing_reoptimization=[],
            tool_results=[],
            is_complete=False,
            errors=[]
        )
        
        try:
            # Execute the LangGraph workflow
            config = {"thread_id": f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"}
            final_state = self.workflow.invoke(initial_state, config)
            
            logger.info("=" * 80)
            
            # Return results
            if final_state.get("optimization_report"):
                logger.info("🎉 LangGraph analysis completed successfully!")
                return final_state["optimization_report"]
            else:
                # Check if 0 queries were found (valid scenario) vs actual failure
                query_history = final_state.get("query_history", [])
                query_history_errors = [error for error in final_state.get("errors", []) if "Query history failed" in error]
                
                if len(query_history) == 0 and len(query_history_errors) == 0:
                    # 0 queries found - this is a valid informational result, not an error
                    logger.info("ℹ️ No slow queries found matching the specified criteria")
                    
                    # Generate appropriate session identifier for the response
                    if session_id:
                        analysis_session_id = session_id
                    elif query_tag and start_date:
                        analysis_session_id = f"tag_{query_tag}_date_{start_date}"
                    elif query_id:
                        analysis_session_id = f"query_{query_id}"
                    else:
                        analysis_session_id = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    
                    return {
                        "info": "0 queries found",
                        "session_analysis": {
                            "session_id": analysis_session_id,
                            "analysis_timestamp": datetime.now().isoformat(),
                            "total_queries_analyzed": 0,
                            "total_execution_time_seconds": 0.0
                        },
                        "summary": {
                            "total_bottlenecks_found": 0,
                            "queries_needing_rewrite": 0,
                            "infrastructure_changes_recommended": 0,
                            "estimated_total_improvement": "No slow queries found to optimize"
                        },
                        "message": "No slow queries found matching the specified criteria. This indicates good performance or no recent activity matching the filter criteria.",
                        "analysis_timestamp": datetime.now().isoformat(),
                        "criteria_used": {
                            "session_id": session_id,
                            "query_tag": query_tag,
                            "start_date": start_date,
                            "query_id": query_id,
                            "execution_time_threshold": "> 180000 ms (3 minutes)" if not query_id else "N/A (specific query)"
                        }
                    }
                else:
                    # Actual failure occurred
                    logger.error("❌ Analysis failed to generate optimization report")
                    return {
                        "error": "Analysis incomplete or failed",
                        "session_id": session_id,
                        "query_tag": query_tag,
                        "start_date": start_date,
                        "query_id": query_id,
                        "messages": final_state.get("messages", []),
                        "errors": final_state.get("errors", []),
                        "tool_results": final_state.get("tool_results", []),
                        "analysis_timestamp": datetime.now().isoformat()
                    }
                
        except Exception as e:
            logger.error(f"❌ LangGraph workflow execution failed: {str(e)}")
            return {
                "error": f"Workflow execution failed: {str(e)}",
                "session_id": session_id,
                "query_tag": query_tag,
                "start_date": start_date,
                "query_id": query_id,
                "analysis_timestamp": datetime.now().isoformat()
            }


def create_langgraph_agent_from_env(progress_callback: Optional[Callable[[ProgressUpdate], None]] = None) -> SnowflakePerformanceLangGraphAgent:
    """Factory function to create LangGraph agent with Gemini from environment variables."""
    ai_config = AIConfig.from_env()
    
    return SnowflakePerformanceLangGraphAgent(ai_config, progress_callback)