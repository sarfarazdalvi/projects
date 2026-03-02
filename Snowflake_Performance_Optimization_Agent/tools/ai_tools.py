"""
AI-powered analysis tools using Google Gemini.

This module contains tools that use AI/LLM services to analyze
query performance, generate optimizations, and evaluate semantic equivalence.
"""

import json
import time
import hashlib
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

import google.generativeai as genai

from models.data_models import (
    AIConfig,
    ToolResult,
    QueryProfile,
    QueryAnalysisResult,
    QueryEvaluationResult,
    Bottleneck,
    OptimizationRecommendation,
    InfrastructureChange,
    PerformanceAnalysisData
)
from models.schemas import (
    OPERATOR_STATS_SCHEMA,
    QUERY_PERFORMANCE_SCHEMA,
    OPTIMIZATION_SCHEMA,
    SEMANTIC_EVALUATION_SCHEMA
)
from utils.constants import OPTIMIZATION_RULES_TEXT

logger = logging.getLogger(__name__)


def ask_gemini_with_schema(
    prompt: str,
    client: genai.GenerativeModel,
    schema: Dict[str, Any],
    temperature: float = 0.1,
    max_tokens: int = 8192
) -> Dict[str, Any]:
    """
    LLM call wrapper with structured JSON schema response using Google Gemini.
    
    Args:
        prompt: The prompt to send to the AI
        client: Gemini GenerativeModel instance
        schema: JSON schema for structured response
        temperature: Temperature for response generation
        max_tokens: Maximum tokens in response
        
    Returns:
        Parsed JSON dictionary from structured AI response
        
    Raises:
        RuntimeError: If AI call failed
    """
    # Log request details
    prompt_preview = prompt[:500] + "..." if len(prompt) > 500 else prompt
    logger.info(f"🤖 LLM Request - Model: {client.model_name}, Temperature: {temperature}, Max Tokens: {max_tokens}")
    logger.debug(f"🤖 LLM Request - Prompt Preview: {prompt_preview}")
    logger.debug(f"🤖 LLM Request - Schema: {json.dumps(schema, indent=2)}")
    
    try:
        # Configure the model for structured output
        model = client.with_config(generation_config={
            "temperature": temperature,
            "max_output_tokens": max_tokens
        }, response_schema=schema)
        
        response = model.generate_content(prompt)
        
        logger.info(f"🤖 LLM Raw Response: {response}")
        
        # Access the structured part of the response
        parsed_response = response.candidates[0].content.parts[0].function_call.args
        
        logger.info(f"🤖 LLM Response - Successfully parsed JSON with {len(parsed_response)} top-level keys")
        logger.debug(f"🤖 LLM Parsed Response: {json.dumps(parsed_response, indent=2)}")
        return parsed_response
            
    except Exception as e:
        logger.error(f"🤖 LLM Request - Gemini call failed: {e}")
        logger.error(f"🤖 LLM Request - Prompt that failed: {prompt_preview}")
        raise RuntimeError(f"AI call failed: {e}")


class OperatorStatsAnalysisTool:
    """Tool for AI-powered operator statistics analysis to identify top bottlenecks."""
    
    def __init__(self, ai_config: AIConfig):
        self.config = ai_config
        genai.configure(api_key=ai_config.gemini_api_key)
        self.client = genai.GenerativeModel(ai_config.model_name)
        self.name = "operator_stats_analysis"
    
    def __call__(self, query_profiles: Dict[str, QueryProfile]) -> ToolResult:
        """Analyze operator statistics to identify top bottlenecks for each query."""
        start_time = time.time()
        
        try:
            logger.info(f"🔍 Analyzing operator statistics for {len(query_profiles)} queries...")
            
            bottleneck_analyses = {}
            failed_queries = []  # Track AI call failures
            ai_error_details = []  # Collect AI error details
            
            for query_id, profile in query_profiles.items():
                operator_stats = profile.operator_stats
                
                if not operator_stats:
                    logger.warning(f"⚠️ No operator stats for query {query_id}")
                    bottleneck_analyses[query_id] = {"top_bottlenecks": [], "analysis_summary": "No operator statistics available"}
                    continue
                
                # Create focused prompt for operator stats analysis
                bottleneck_prompt = f"""
                You are a Snowflake performance expert analyzing operator statistics to identify TOP 3 to 5 specific bottlenecks.
                *MUST DO": Be very concise and limit analysis to top 3 to 5 highest impact operators
                CRITICAL ANALYSIS REQUIREMENTS:
                1. Include PRECISE metrics: execution time %, row counts, data volumes, memory usage
                2. Identify SPECIFIC operation types (TableScan, HashJoin, HashAggregate, Sort, etc.)
                3. Explain EXACTLY what each operator is doing (scanning which table, joining which tables)
                4. Provide CONCRETE performance impact with numbers
                5. Append analysis for each operator_id to top_bottlenecks list
                
                OPERATOR STATISTICS DATA:
                {json.dumps(operator_stats, indent=2)}
                
                From significant bottleneck, extract:
                - Exact operator type (OPERATOR_TYPE field)
                - Specific table names (OPERATOR_ATTRIBUTES:table_name)
                - Execution time percentage (EXECUTION_TIME_BREAKDOWN:overall_percentage)
                - Row counts processed (INPUT_ROWS, OUTPUT_ROWS)
                - Memory usage (BYTES_ASSIGNED, BYTES_SPILLED_TO_LOCAL_STORAGE)
                - Data volume (BYTES_READ_FROM_RESULT, BYTES_SENT_NETWORK)
                
                Return in this EXACT JSON format:
                {{
                    "top_bottlenecks": [
                        {{
                            "type": "table_scan",
                            "description": "TableScan operator scanning EXAMPLE_DB.SALES.ORDERS table processed 1,665,492,149 rows across 13,158 micro-partitions",
                            "severity": "critical",
                            "impact": "24% of total query time",
                            "specific_operation": "Full table scan on EXAMPLE_DB.SALES.ORDERS",
                            "affected_tables": ["EXAMPLE_DB.SALES.ORDERS"],
                            "performance_metrics": {{
                                "execution_time_percentage": "45.84%",
                                "rows_processed": "1,665,492,149",
                                "data_volume": "125.3 GB",
                                "memory_spilled": "0 GB",
                                "micro_partitions_scanned": "13,158"
                            }},
                            "root_cause": "No clustering key or partition pruning on date-filtered queries"
                        }},
                        {{
                            "type": "join_operation",
                            "description": "HashJoin operator joining ORDERS with CUSTOMER tables processed 2.1 billion input rows",
                            "severity": "high",
                            "impact": "16% of total query time",
                            "specific_operation": "Hash join between EXAMPLE_DB.SALES.ORDERS and EXAMPLE_DB.SALES.CUSTOMER",
                            "affected_tables": ["EXAMPLE_DB.SALES.ORDERS", "EXAMPLE_DB.SALES.CUSTOMER"],
                            "performance_metrics": {{
                                "execution_time_percentage": "22.43%",
                                "input_rows": "2,100,000,000",
                                "output_rows": "645,000,000",
                                "memory_usage": "24.7 GB",
                                "memory_spilled": "15.1 GB"
                            }},
                            "root_cause": "Large fact table join without sufficient memory allocation"
                        }}
                    ],
                    "analysis_summary": "Query performance dominated by full table scan on 1.66B row ORDERS table (45.84% time) followed by memory-constrained join operations (22.43% time)"
                }}
                
                EXTRACT REAL DATA from the operator statistics - do not use placeholder values. Be extremely specific and concise.
                ** Report ONLY top 3 to 5 highest impact steps/operators **.
                """
                
                try:
                    # Use structured JSON schema response - guaranteed valid JSON
                    analysis_data = ask_gemini_with_schema(
                        prompt=bottleneck_prompt,
                        client=self.client,
                        schema=OPERATOR_STATS_SCHEMA,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_tokens
                    )
                    bottleneck_analyses[query_id] = analysis_data
                    logger.info(f"✅ Analyzed operator stats for query {query_id[:8]}...")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Operator analysis failed for query {query_id}: {str(e)}")
                    bottleneck_analyses[query_id] = {"top_bottlenecks": [], "analysis_summary": f"Analysis failed: {str(e)}"}
                    failed_queries.append(query_id)
                    ai_error_details.append(f"Query {query_id[:8]}: AI call failed - {str(e)}")
            
            execution_time = (time.time() - start_time) * 1000
            
            # Determine success based on AI call results
            if failed_queries:
                # Some AI calls failed - determine if this should be a failure
                success_rate = (len(query_profiles) - len(failed_queries)) / len(query_profiles)
                
                if success_rate == 0:
                    # All AI calls failed - this is a complete failure
                    logger.error(f"❌ All operator stats analysis AI calls failed")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"bottleneck_analyses": {}, "query_count": 0},
                        error_message=f"All AI analysis calls failed. Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                elif success_rate < 0.5:
                    # More than half failed - this is a significant failure
                    logger.error(f"❌ Majority of operator stats analysis AI calls failed ({len(failed_queries)}/{len(query_profiles)})")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"bottleneck_analyses": bottleneck_analyses, "query_count": len(query_profiles)},
                        error_message=f"Majority of AI analysis calls failed ({len(failed_queries)}/{len(query_profiles)}). Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                else:
                    # Partial failure - continue with warning
                    logger.warning(f"⚠️ Some operator stats analysis AI calls failed ({len(failed_queries)}/{len(query_profiles)})")
                    return ToolResult(
                        success=True,
                        tool_name=self.name,
                        data={"bottleneck_analyses": bottleneck_analyses, "query_count": len(query_profiles), "ai_failures": ai_error_details},
                        execution_time_ms=execution_time
                    )
            
            logger.info(f"✅ Operator stats analysis completed in {execution_time:.2f}ms")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={"bottleneck_analyses": bottleneck_analyses, "query_count": len(query_profiles)},
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Operator stats analysis failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"bottleneck_analyses": {}, "query_count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class QueryPerformanceAnalysisTool:
    """Tool for AI-powered query performance analysis focused on bottleneck identification."""
    
    def __init__(self, ai_config: AIConfig):
        self.config = ai_config
        genai.configure(api_key=ai_config.gemini_api_key)
        self.client = genai.GenerativeModel(ai_config.model_name)
        self.name = "query_performance_analysis"
    
    def __call__(self, queries: List[Dict[str, Any]], profiles: Dict[str, QueryProfile], bottleneck_analyses: Optional[Dict[str, Any]] = None) -> ToolResult:
        """Perform focused query performance analysis to identify detailed bottlenecks."""
        start_time = time.time()
        
        try:
            logger.info(f"📈 Performing query performance analysis on {len(queries)} queries...")
            
            performance_analyses = {}
            failed_queries = []  # Track AI call failures
            ai_error_details = []  # Collect AI error details
            
            for query_data in queries:
                query_id = query_data['query_id']
                query_text = query_data['query_text']
                execution_stats = query_data['execution_stats']
                query_profile = profiles.get(query_id)
                
                # Get pre-analyzed bottlenecks if available
                pre_analyzed_bottlenecks = None
                if bottleneck_analyses and query_id in bottleneck_analyses:
                    pre_analyzed_bottlenecks = bottleneck_analyses[query_id]
                
                # Create focused performance analysis prompt
                if pre_analyzed_bottlenecks and pre_analyzed_bottlenecks.get('top_bottlenecks'):
                    analysis_prompt = f"""
                    You are a Snowflake SQL performance optimization expert. Analyze performance issues with below query with focus on detailed bottleneck analysis and performance characterization.
                    
                    CRITICAL REQUIREMENTS:
                    - NEVER use vague terms like "This operation" or "The operation"
                    - ALWAYS specify the exact Snowflake operator name and table names
                    - Include specific row counts, data volumes, and timing details
                    - Reference actual table names from the query when possible
                    - Do not merge performance analysis from multiple operator_ids together
                    - Combine performance analysis and performance issues into single comprehensive analysis
                    
                    
                    QUERY:
                    {query_text}
                    
                    EXECUTION STATS:
                    {json.dumps(execution_stats, indent=2)}
                    
                    PRE-IDENTIFIED TOP BOTTLENECKS (from operator statistics):
                    {json.dumps(pre_analyzed_bottlenecks, indent=2)}
                    
                    Provide comprehensive performance analysis in JSON format:
                    {{
                        "performance_analysis": [
                            {{
                                "type": "table_scan",
                                "description": "TableScan operator on CUSTOMER_DATA table scanned 2M rows without clustering key",
                                "severity": "high",
                                "impact": "TableScan operator consumed 27.18% of overall execution time and spilled 56.5 GB of data to local disk",
                                "root_cause": "Missing clustering key on frequently filtered DATE_COLUMN in CUSTOMER_DATA table",
                                "affected_tables": ["CUSTOMER_DATA"],
                                "resource_impact": {{"cpu": "high", "io": "high", "memory": "medium"}}
                            }},
                            {{
                                "type": "join_operation",
                                "description": "HashJoin operator between ORDERS and CUSTOMER tables with memory spill",
                                "severity": "medium",
                                "impact": "HashJoin operator consumed 24.72% of overall execution time, with 15.9% on processing and 7.78% on network communication",
                                "root_cause": "Large fact table join without proper filtering causing memory pressure",
                                "affected_tables": ["ORDERS", "CUSTOMER"],
                                "resource_impact": {{"cpu": "medium", "io": "medium", "memory": "high"}}
                            }}
                        ],
                        "query_characteristics": {{
                            "complexity": "high",
                            "data_volume": "Processing 1.75 billion rows from ORDERS table joined with CUSTOMER table",
                            "join_complexity": "Multiple hash joins with large fact table causing memory spill",
                            "aggregation_intensity": "Heavy GROUP BY operations on high-cardinality dimensions"
                        }},
                        "performance_metrics": {{
                            "cpu_utilization": "High CPU usage from TableScan and HashJoin operators",
                            "io_pattern": "Sequential table scans with significant remote disk I/O (11.88% of execution time)",
                            "memory_pressure": "Memory spill to local disk (56.5 GB spilled) due to large HashJoin operations",
                            "spill_to_disk": "Yes, 56.5 GB spilled to local disk from memory-intensive operations"
                        }},
                        "estimated_performance_gain_potential": "50-70% improvement possible with clustering keys and query optimization"
                    }}
                    
                    Focus on understanding WHY the query performs poorly with specific operator and table details.
                    """
                else:
                    # Fallback to comprehensive performance analysis
                    operator_stats = query_profile.operator_stats if query_profile else []
                    analysis_prompt = f"""
                    You are a Snowflake SQL performance expert. Analyze query performance and identify bottlenecks.
                    
                    CRITICAL REQUIREMENTS:
                    - NEVER use vague terms like "This operation" or "The operation"
                    - ALWAYS specify exact Snowflake operator names and table names from the query
                    - Include specific row counts, data volumes, and timing details when available
                    - Combine performance analysis and performance issues into single comprehensive analysis
                    
                    QUERY:
                    {query_text}
                    
                    EXECUTION STATS:
                    {json.dumps(execution_stats, indent=2)}
                    
                    OPERATOR STATS:
                    {json.dumps(operator_stats, indent=2) if operator_stats else "No operator statistics available"}
                    
                    Provide comprehensive performance analysis in JSON format:
                    {{
                        "performance_analysis": [
                            {{
                                "type": "table_scan",
                                "description": "TableScan operator on [SPECIFIC_TABLE_NAME] detected full table scan of 2M rows",
                                "severity": "high",
                                "impact": "TableScan operator consumed 45% of execution time scanning entire table",
                                "root_cause": "Missing clustering key on [SPECIFIC_COLUMN_NAME] in [TABLE_NAME] table",
                                "affected_tables": ["SPECIFIC_TABLE_NAME"],
                                "resource_impact": {{"cpu": "medium", "io": "high", "memory": "low"}}
                            }}
                        ],
                        "query_characteristics": {{
                            "complexity": "medium",
                            "data_volume": "Processing 2M rows from [SPECIFIC_TABLE_NAMES]",
                            "join_complexity": "HashJoin operators between [TABLE1] and [TABLE2] with large fact table",
                            "aggregation_intensity": "GroupBy operators processing multiple high-cardinality dimensions"
                        }},
                        "performance_metrics": {{
                            "cpu_utilization": "60% average from TableScan and HashJoin operators",
                            "io_pattern": "Sequential table scans with some random access from join operations",
                            "memory_pressure": "Moderate spill to local storage from HashJoin operators",
                            "spill_to_disk": "Yes, 500MB spilled from memory-intensive HashJoin operations"
                        }},
                        "estimated_performance_gain_potential": "40-60% improvement possible with clustering keys and query optimization"
                    }}
                    
                    Focus on understanding performance characteristics with specific operator and table details.
                    """
                
                try:
                    # Use structured JSON schema response - guaranteed valid JSON
                    analysis_data = ask_gemini_with_schema(
                        prompt=analysis_prompt,
                        client=self.client,
                        schema=QUERY_PERFORMANCE_SCHEMA,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_tokens
                    )
                    performance_analyses[query_id] = analysis_data
                    logger.info(f"✅ Analyzed performance for query {query_id[:8]}...")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Performance analysis failed for query {query_id}: {str(e)}")
                    performance_analyses[query_id] = {"performance_analysis": [], "query_characteristics": {}, "performance_metrics": {}, "error": str(e)}
                    failed_queries.append(query_id)
                    ai_error_details.append(f"Query {query_id[:8]}: AI call failed - {str(e)}")
            
            execution_time = (time.time() - start_time) * 1000
            
            # Determine success based on AI call results (same pattern as OperatorStatsAnalysisTool)
            if failed_queries:
                success_rate = (len(queries) - len(failed_queries)) / len(queries)
                
                if success_rate == 0:
                    logger.error(f"❌ All query performance analysis AI calls failed")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"performance_analyses": {}, "query_count": 0},
                        error_message=f"All AI analysis calls failed. Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                elif success_rate < 0.5:
                    logger.error(f"❌ Majority of query performance analysis AI calls failed ({len(failed_queries)}/{len(queries)})")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"performance_analyses": performance_analyses, "query_count": len(queries)},
                        error_message=f"Majority of AI analysis calls failed ({len(failed_queries)}/{len(queries)}). Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                else:
                    logger.warning(f"⚠️ Some query performance analysis AI calls failed ({len(failed_queries)}/{len(queries)})")
                    return ToolResult(
                        success=True,
                        tool_name=self.name,
                        data={"performance_analyses": performance_analyses, "query_count": len(queries), "ai_failures": ai_error_details},
                        execution_time_ms=execution_time
                    )
            
            logger.info(f"✅ Query performance analysis completed in {execution_time:.2f}ms")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={"performance_analyses": performance_analyses, "query_count": len(queries)},
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Query performance analysis failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"performance_analyses": {}, "query_count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class OptimizedQueryGenerationTool:
    """Tool for generating optimized queries and infrastructure recommendations."""
    
    def __init__(self, ai_config: AIConfig):
        self.config = ai_config
        genai.configure(api_key=ai_config.gemini_api_key)
        self.client = genai.GenerativeModel(ai_config.model_name)
        self.name = "optimized_query_generation"
    
    def __call__(self, queries: List[Dict[str, Any]], performance_analyses: Dict[str, Any], previous_feedback: Optional[Dict[str, List[str]]] = None, table_details: Optional[Dict[str, Any]] = None) -> ToolResult:
        """Generate optimized queries and infrastructure recommendations based on performance analysis."""
        start_time = time.time()
        
        try:
            logger.info(f"🔧 Generating optimizations for {len(queries)} queries...")
            
            analyses = []
            failed_queries = []
            ai_error_details = []
            
            for query_data in queries:
                query_id = query_data['query_id']
                query_text = query_data['query_text']
                execution_stats = query_data['execution_stats']
                
                # Get performance analysis results
                perf_analysis = performance_analyses.get(query_id, {})
                
                # Get table details for this query if available
                query_table_details = table_details.get(query_id, []) if table_details else []
                
                # Get previous feedback for this query if available
                prev_feedback = ""
                if previous_feedback and query_id in previous_feedback:
                    prev_feedback = f"\n\nPREVIOUS OPTIMIZATION FEEDBACK FROM FAILED EVALUATIONS:\n" + "\n".join(previous_feedback[query_id])
                    prev_feedback += "\n\nPLEASE ADDRESS THE ABOVE FEEDBACK IN THIS OPTIMIZATION ATTEMPT.\n"
                
                # Create focused optimization prompt
                optimization_prompt = f"""
                You are a Snowflake SQL optimization expert. Generate specific optimization recommendations and infrastructure changes.
                
                {OPTIMIZATION_RULES_TEXT}
                
                ORIGINAL QUERY:
                {query_text}
                
                EXECUTION STATS:
                {json.dumps(execution_stats, indent=2)}
                
                PERFORMANCE ANALYSIS RESULTS:
                {json.dumps(perf_analysis, indent=2)}
                
                TABLE STATISTICS AND CLUSTERING INFORMATION:
                {json.dumps(query_table_details, indent=2) if query_table_details else "No table details available"}
                {prev_feedback}
                Generate optimization recommendations in JSON format:
                {{
                    "optimization_recommendations": [
                        {{
                            "type": "query_rewrite",
                            "description": "Specific SQL optimization recommendation",
                            "expected_improvement": "20-30% faster execution",
                            "ddl_suggestion": "Actual optimized SQL query",
                            "priority": "high"
                        }},
                        {{
                            "type": "clustering",
                            "description": "Add clustering key on commonly filtered columns",
                            "expected_improvement": "40-50% faster execution",
                            "ddl_suggestion": "ALTER TABLE table_name CLUSTER BY (date_column, category_column)",
                            "priority": "medium"
                        }}
                    ],
                    "query_rewrite_needed": true,
                    "optimized_query": "SELECT specific_columns FROM table WHERE optimized_conditions",
                    "infrastructure_changes": [
                        {{
                            "type": "warehouse_sizing",
                            "recommendation": "Upgrade to LARGE warehouse for CPU-intensive operations",
                            "justification": "Query shows high CPU utilization with complex aggregations",
                            "estimated_cost_impact": "2x compute cost, 3x performance improvement"
                        }},
                        {{
                            "type": "table_optimization",
                            "recommendation": "Enable Search Optimization Service for point lookups",
                            "justification": "Query performs selective filters on large tables",
                            "estimated_cost_impact": "Additional storage cost, 5-10x faster point lookups"
                        }}
                    ],
                    "estimated_performance_gain": "50-70% overall improvement"
                }}
                
                Focus on actionable, specific recommendations with concrete DDL statements and infrastructure changes.
                Include both query-level and table-level optimizations with cost-benefit analysis.
                """
                
                try:
                    # Use structured JSON schema response - guaranteed valid JSON
                    optimization_data = ask_gemini_with_schema(
                        prompt=optimization_prompt,
                        client=self.client,
                        schema=OPTIMIZATION_SCHEMA,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_tokens
                    )
                    
                    # Combine performance analysis bottlenecks with optimization data
                    bottlenecks = []
                    # Handle both old 'bottlenecks' format and new 'performance_analysis' format
                    performance_data = perf_analysis.get('performance_analysis', perf_analysis.get('bottlenecks', []))
                    if performance_data:
                        for bottleneck_data in performance_data:
                            bottlenecks.append(Bottleneck(**bottleneck_data))
                    
                    # Parse optimization recommendations
                    optimization_recommendations = [
                        OptimizationRecommendation(**rec)
                        for rec in optimization_data.get('optimization_recommendations', [])
                    ]
                    
                    # Parse infrastructure changes
                    infrastructure_changes = [
                        InfrastructureChange(**change)
                        for change in optimization_data.get('infrastructure_changes', [])
                    ]
                    
                    # Create QueryAnalysisResult object
                    analysis = QueryAnalysisResult(
                        query_id=query_id,
                        query_hash=hashlib.sha256(query_text.encode()).hexdigest()[:16],
                        original_query_text=query_text,
                        execution_time_seconds=execution_stats.get('execution_time_ms', 0) / 1000.0,
                        bottlenecks=bottlenecks,
                        optimization_recommendations=optimization_recommendations,
                        query_rewrite_needed=optimization_data.get('query_rewrite_needed', False),
                        optimized_query=optimization_data.get('optimized_query'),
                        infrastructure_changes=infrastructure_changes,
                        estimated_performance_gain=optimization_data.get('estimated_performance_gain', 'Unknown'),
                        analysis_timestamp=datetime.now().isoformat()
                    )
                    
                    analyses.append(analysis)
                    logger.info(f"✅ Generated optimizations for query {query_id[:8]}...")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Optimization generation failed for query {query_id}: {str(e)}")
                    failed_queries.append(query_id)
                    ai_error_details.append(f"Query {query_id[:8]}: AI call failed - {str(e)}")
                    
                    # Create basic analysis with empty results
                    analysis = QueryAnalysisResult(
                        query_id=query_id,
                        query_hash=hashlib.sha256(query_text.encode()).hexdigest()[:16],
                        original_query_text=query_text,
                        execution_time_seconds=execution_stats.get('execution_time_ms', 0) / 1000.0,
                        bottlenecks=[],
                        optimization_recommendations=[],
                        query_rewrite_needed=False,
                        optimized_query=None,
                        infrastructure_changes=[],
                        estimated_performance_gain="Optimization generation failed",
                        analysis_timestamp=datetime.now().isoformat()
                    )
                    analyses.append(analysis)
            
            execution_time = (time.time() - start_time) * 1000
            
            # Determine success based on AI call results (same pattern as other AI tools)
            if failed_queries:
                success_rate = (len(queries) - len(failed_queries)) / len(queries)
                
                if success_rate == 0:
                    logger.error(f"❌ All optimization generation AI calls failed")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"analyses": [], "query_count": 0},
                        error_message=f"All AI optimization calls failed. Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                elif success_rate < 0.5:
                    logger.error(f"❌ Majority of optimization generation AI calls failed ({len(failed_queries)}/{len(queries)})")
                    performance_data = PerformanceAnalysisData(
                        analyses=analyses,
                        query_count=len(analyses),
                        analysis_timestamp=datetime.now().isoformat()
                    )
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data=performance_data.model_dump(),
                        error_message=f"Majority of AI optimization calls failed ({len(failed_queries)}/{len(queries)}). Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                else:
                    logger.warning(f"⚠️ Some optimization generation AI calls failed ({len(failed_queries)}/{len(queries)})")
                    performance_data = PerformanceAnalysisData(
                        analyses=analyses,
                        query_count=len(analyses),
                        analysis_timestamp=datetime.now().isoformat()
                    )
                    result_data = performance_data.model_dump()
                    result_data["ai_failures"] = ai_error_details
                    return ToolResult(
                        success=True,
                        tool_name=self.name,
                        data=result_data,
                        execution_time_ms=execution_time
                    )
            
            logger.info(f"✅ Optimized query generation completed in {execution_time:.2f}ms")
            
            # Create PerformanceAnalysisData object
            performance_data = PerformanceAnalysisData(
                analyses=analyses,
                query_count=len(analyses),
                analysis_timestamp=datetime.now().isoformat()
            )
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data=performance_data.model_dump(),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Optimized query generation failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"analyses": [], "query_count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class QuerySemanticEvaluationTool:
    """Tool for evaluating semantic equivalence between original and optimized queries."""
    
    def __init__(self, ai_config: AIConfig):
        self.config = ai_config
        genai.configure(api_key=ai_config.gemini_api_key)
        self.client = genai.GenerativeModel(ai_config.model_name)
        self.name = "query_semantic_evaluation"
    
    def __call__(self, analyses: List[QueryAnalysisResult], previous_feedback: Optional[Dict[str, List[str]]] = None) -> ToolResult:
        """Evaluate semantic equivalence of optimized queries and provide feedback for improvements."""
        start_time = time.time()
        
        try:
            logger.info(f"🔍 Evaluating semantic equivalence for {len(analyses)} optimized queries...")
            
            evaluations = {}
            failed_queries = []
            ai_error_details = []
            
            for analysis in analyses:
                query_id = analysis.query_id
                original_query = analysis.original_query_text
                optimized_query = analysis.optimized_query
                
                # Handle empty optimized query case - set semantic_equivalence to True to allow agent progression
                if not optimized_query or not analysis.query_rewrite_needed:
                    logger.info(f"⏭️ No query rewrite needed for query {query_id[:8]} - setting semantic equivalence to True")
                    
                    # Create a positive evaluation result to allow the agent to proceed
                    evaluations[query_id] = QueryEvaluationResult(
                        query_id=query_id,
                        semantic_equivalence=True,
                        confidence_score=1.0,
                        differences_found=["No query rewrite was needed - original query is acceptable"],
                        recommendation="ACCEPT",
                        feedback_for_optimization=None,
                        evaluation_timestamp=datetime.now().isoformat()
                    )
                    continue
                
                # Get previous feedback for this query if available
                prev_feedback = ""
                if previous_feedback and query_id in previous_feedback:
                    prev_feedback = f"\n\nPREVIOUS FEEDBACK FROM ITERATIONS:\n" + "\n".join(previous_feedback[query_id])
                
                # Create semantic evaluation prompt
                evaluation_prompt = f"""
                You are a Analytics Data Engineer. Analyze whether two SQL queries will produce the same results.
                
                ORIGINAL QUERY:
                {original_query}
                
                OPTIMIZED QUERY:
                {optimized_query}
                
                OPTIMIZATION CONTEXT:
                - Query ID: {query_id}
                - Performance bottlenecks addressed: {[b.type for b in analysis.bottlenecks]}
                - Optimization recommendations applied: {[r.type for r in analysis.optimization_recommendations]}
                {prev_feedback}
                
                Evaluate semantic equivalence and provide analysis in JSON format:
                {{
                    "semantic_equivalence": true/false,
                    "confidence_score": 0.95,
                    "differences_found": [
                        "Column order changed but semantically equivalent",
                        "Added explicit JOIN conditions for clarity",
                        "WHERE clause reorganized for better performance"
                    ],
                    "recommendation": "ACCEPT|REJECT|RETRY_WITH_FEEDBACK",
                    "feedback_for_optimization": "Specific feedback to improve the optimization if RETRY_WITH_FEEDBACK",
                    "detailed_analysis": {{
                        "select_clause_equivalent": true,
                        "where_clause_equivalent": true,
                        "join_logic_equivalent": true,
                        "grouping_equivalent": true,
                        "ordering_equivalent": true,
                        "potential_result_differences": []
                    }}
                }}
                
                Evaluation criteria:
                1. Both queries must return the same result set (same rows and columns)
                2. Column order differences are acceptable if data is identical
                3. Performance optimizations that don't change logic are acceptable
                4. Different execution paths that yield same results are acceptable
                5. Be strict about logical differences that could change results
                
                If semantic_equivalence is false, provide specific feedback for optimization retry.
                If confidence_score < 0.8, recommend RETRY_WITH_FEEDBACK even if equivalent.
                """
                
                try:
                    # Use structured JSON schema response - guaranteed valid JSON
                    eval_data = ask_gemini_with_schema(
                        prompt=evaluation_prompt,
                        client=self.client,
                        schema=SEMANTIC_EVALUATION_SCHEMA,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_tokens
                    )
                    
                    # Create QueryEvaluationResult
                    evaluation = QueryEvaluationResult(
                        query_id=query_id,
                        semantic_equivalence=eval_data.get('semantic_equivalence', False),
                        confidence_score=eval_data.get('confidence_score', 0.0),
                        differences_found=eval_data.get('differences_found', []),
                        recommendation=eval_data.get('recommendation', 'REJECT'),
                        feedback_for_optimization=eval_data.get('feedback_for_optimization'),
                        evaluation_timestamp=datetime.now().isoformat()
                    )
                    
                    evaluations[query_id] = evaluation
                    
                    # Log evaluation result
                    equivalence_status = "✅" if evaluation.semantic_equivalence else "❌"
                    logger.info(f"{equivalence_status} Query {query_id[:8]} evaluation: "
                              f"equivalent={evaluation.semantic_equivalence}, "
                              f"confidence={evaluation.confidence_score:.2f}, "
                              f"recommendation={evaluation.recommendation}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Evaluation failed for query {query_id}: {str(e)}")
                    failed_queries.append(query_id)
                    ai_error_details.append(f"Query {query_id[:8]}: AI call failed - {str(e)}")
                    
                    evaluations[query_id] = QueryEvaluationResult(
                        query_id=query_id,
                        semantic_equivalence=False,
                        confidence_score=0.0,
                        differences_found=[f"Evaluation error: {str(e)}"],
                        recommendation="REJECT",
                        feedback_for_optimization=f"Evaluation failed due to error: {str(e)}",
                        evaluation_timestamp=datetime.now().isoformat()
                    )
            
            execution_time = (time.time() - start_time) * 1000
            
            # Calculate summary statistics
            total_evaluations = len(evaluations)
            accepted_queries = len([e for e in evaluations.values() if e.recommendation == "ACCEPT"])
            rejected_queries = len([e for e in evaluations.values() if e.recommendation == "REJECT"])
            retry_queries = len([e for e in evaluations.values() if e.recommendation == "RETRY_WITH_FEEDBACK"])
            
            # Determine success based on AI call results (same pattern as other AI tools)
            if failed_queries:
                success_rate = (len(analyses) - len(failed_queries)) / len(analyses)
                
                if success_rate == 0:
                    logger.error(f"❌ All semantic evaluation AI calls failed")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={"evaluations": {}, "summary": {}},
                        error_message=f"All AI evaluation calls failed. Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                elif success_rate < 0.5:
                    logger.error(f"❌ Majority of semantic evaluation AI calls failed ({len(failed_queries)}/{len(analyses)})")
                    return ToolResult(
                        success=False,
                        tool_name=self.name,
                        data={
                            "evaluations": {k: v.model_dump() for k, v in evaluations.items()},
                            "summary": {
                                "total_evaluations": total_evaluations,
                                "accepted_queries": accepted_queries,
                                "rejected_queries": rejected_queries,
                                "retry_queries": retry_queries
                            }
                        },
                        error_message=f"Majority of AI evaluation calls failed ({len(failed_queries)}/{len(analyses)}). Errors: {'; '.join(ai_error_details)}",
                        execution_time_ms=execution_time
                    )
                else:
                    logger.warning(f"⚠️ Some semantic evaluation AI calls failed ({len(failed_queries)}/{len(analyses)})")
                    result_data = {
                        "evaluations": {k: v.model_dump() for k, v in evaluations.items()},
                        "summary": {
                            "total_evaluations": total_evaluations,
                            "accepted_queries": accepted_queries,
                            "rejected_queries": rejected_queries,
                            "retry_queries": retry_queries
                        },
                        "ai_failures": ai_error_details
                    }
                    return ToolResult(
                        success=True,
                        tool_name=self.name,
                        data=result_data,
                        execution_time_ms=execution_time
                    )
            
            logger.info(f"✅ Semantic evaluation completed in {execution_time:.2f}ms")
            logger.info(f"📊 Evaluation Summary: Total={total_evaluations}, "
                       f"Accepted={accepted_queries}, Rejected={rejected_queries}, Retry={retry_queries}")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={
                    "evaluations": {k: v.model_dump() for k, v in evaluations.items()},
                    "summary": {
                        "total_evaluations": total_evaluations,
                        "accepted_queries": accepted_queries,
                        "rejected_queries": rejected_queries,
                        "retry_queries": retry_queries
                    }
                },
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Semantic evaluation failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"evaluations": {}, "summary": {}},
                error_message=str(e),
                execution_time_ms=execution_time
            )