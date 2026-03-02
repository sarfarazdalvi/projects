"""
Snowflake-specific tools for data collection and report generation.

This module contains tools that interact directly with Snowflake
to fetch query history, profile queries, get table details, and generate reports.
"""

import time
import logging
from typing import List, Optional
from datetime import datetime

from snowflake_connector import SnowflakeConnector
from models.data_models import (
    QueryInfo, 
    QueryProfile, 
    ToolResult,
    QueryAnalysisResult,
    SessionAnalysisSummary,
    AnalysisSummary,
    AggregatedRecommendations,
    OptimizationReport
)

logger = logging.getLogger(__name__)


class QueryHistoryTool:
    """Tool for fetching query history using generic Snowflake connection."""
    
    def __init__(self):
        self.name = "query_history"
    
    def __call__(self, session_id: str = None, query_tag: str = None, start_date: str = None, query_id: str = None, limit: int = 5) -> ToolResult:
        """Fetch query history for a session, by query tag and start date, or specific query ID."""
        start_time = time.time()
        
        try:
            if session_id:
                logger.info(f"📊 Fetching query history for session {session_id}...")
            elif query_tag and start_date:
                logger.info(f"📊 Fetching query history for tag '{query_tag}' from {start_date}...")
            elif query_id:
                logger.info(f"📊 Fetching specific query {query_id}...")
            else:
                raise ValueError("Either session_id, query_id, or both query_tag and start_date must be provided")
            
            sf_connector = SnowflakeConnector()
            connection = sf_connector.get_connection()
            cursor = connection.cursor()
            
            # Base query structure
            base_query = """
            SELECT
                QUERY_ID,
                QUERY_TEXT,
                QUERY_TYPE,
                DATABASE_NAME,
                SCHEMA_NAME,
                SESSION_ID,
                USER_NAME,
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE,
                EXECUTION_STATUS,
                TOTAL_ELAPSED_TIME,
                COMPILATION_TIME,
                EXECUTION_TIME,
                QUERY_TAG,
                START_TIME
            FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY"
            """
            
            # Dynamic WHERE clause and parameters based on input method
            if session_id:
                # Original session-based filtering
                where_clause = """
                WHERE SESSION_ID = %s
                AND EXECUTION_TIME > 180000
                ORDER BY START_TIME ASC
                """
                params = (session_id,)
            elif query_id:
                # Specific query ID filtering
                where_clause = """
                WHERE QUERY_ID = %s
                ORDER BY START_TIME ASC
                """
                params = (query_id,)
            else:
                # Query tag and date-based filtering
                where_clause = """
                WHERE QUERY_TAG = %s
                AND TO_DATE(START_TIME) = %s
                AND EXECUTION_TIME > 180000
                ORDER BY START_TIME ASC
                """
                params = (query_tag, start_date)
            
            # Complete query
            history_query = base_query + where_clause
            
            cursor.execute(history_query, params)
            results = cursor.fetchall()
            
            # Convert to list of dicts
            column_names = [desc[0] for desc in cursor.description]
            results = [dict(zip(column_names, row)) for row in results]
            
            query_history = []
            for result in results:
                execution_stats = {
                    'query_type': result['QUERY_TYPE'],
                    'database_name': result['DATABASE_NAME'],
                    'schema_name': result['SCHEMA_NAME'],
                    'session_id': result['SESSION_ID'],
                    'user_name': result['USER_NAME'],
                    'warehouse_name': result['WAREHOUSE_NAME'],
                    'warehouse_size': result['WAREHOUSE_SIZE'],
                    'execution_status': result['EXECUTION_STATUS'],
                    'total_elapsed_time_ms': result['TOTAL_ELAPSED_TIME'],
                    'compilation_time_ms': result['COMPILATION_TIME'],
                    'execution_time_ms': result['EXECUTION_TIME']
                }
                
                query_info = QueryInfo(
                    query_id=result['QUERY_ID'],
                    query_text=result['QUERY_TEXT'],
                    execution_stats=execution_stats,
                    session_id=session_id
                )
                query_history.append(query_info)
            
            cursor.close()
            connection.close()
            
            execution_time = (time.time() - start_time) * 1000
            logger.info(f"✅ Found {len(query_history)} slow queries in {execution_time:.2f}ms")
            
            if query_history:
                logger.info("="*80)
                logger.info(f"Sample Query: \n{query_history[0].query_text[:200]}...")
                logger.info("="*80)
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={"queries": [q.model_dump() for q in query_history], "count": len(query_history)},
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Query history fetch failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"queries": [], "count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class QueryProfilingTool:
    """Tool for getting detailed query execution profiles using generic Snowflake connection."""
    
    def __init__(self):
        self.name = "query_profiling"
    
    def __call__(self, query_ids: List[str]) -> ToolResult:
        """Get detailed profiling information for queries."""
        start_time = time.time()
        
        try:
            logger.info(f"🔍 Profiling {len(query_ids)} queries...")
            
            sf_connector = SnowflakeConnector()
            connection = sf_connector.get_connection()
            cursor = connection.cursor()
            
            profiles = {}
            for query_id in query_ids:
                try:
                    profile_query = "SELECT * FROM TABLE(GET_QUERY_OPERATOR_STATS(%s))"
                    cursor.execute(profile_query, (query_id,))
                    profile_results = cursor.fetchall()
                    
                    # Convert to list of dicts
                    column_names = [desc[0] for desc in cursor.description]
                    profile_results = [dict(zip(column_names, row)) for row in profile_results]
                    
                    # Create QueryProfile object
                    query_profile = QueryProfile(
                        query_id=query_id,
                        operator_stats=profile_results,
                        resource_usage={}  # Can be populated with additional metrics
                    )
                    profiles[query_id] = query_profile
                    logger.info(f"✅ Profiled query {query_id[:8]}...")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to profile query {query_id}: {str(e)}")
                    # Create empty profile for failed queries
                    profiles[query_id] = QueryProfile(
                        query_id=query_id,
                        operator_stats=[],
                        resource_usage={}
                    )
            
            cursor.close()
            connection.close()
            
            execution_time = (time.time() - start_time) * 1000
            logger.info(f"✅ Profiling completed in {execution_time:.2f}ms")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={"profiles": {k: v.model_dump() for k, v in profiles.items()}, "query_count": len(query_ids)},
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Query profiling failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"profiles": {}, "query_count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class QueryObjectDetailsTool:
    """Tool for fetching table statistics and clustering information using generic Snowflake connection."""
    
    def __init__(self):
        self.name = "query_object_details"
    
    def __call__(self, query_ids: List[str]) -> ToolResult:
        """Get table statistics and clustering information for all tables used in the queries."""
        start_time = time.time()
        
        try:
            logger.info(f"📊 Fetching table object details for {len(query_ids)} queries...")
            
            sf_connector = SnowflakeConnector()
            connection = sf_connector.get_connection()
            cursor = connection.cursor()
            
            all_table_details = {}
            
            for query_id in query_ids:
                try:
                    # SQL query to get table details for a specific query
                    table_details_query = """
                    WITH BASE_TABLES_LIST AS (
                        -- Step 1: Extract the distinct fully qualified table names from the Query Profile
                        SELECT DISTINCT
                            OPERATOR_ATTRIBUTES:table_name::STRING AS fully_qualified_table_name
                        FROM
                            TABLE(GET_QUERY_OPERATOR_STATS(%s))
                        WHERE
                            -- Filter for operators that represent a physical read from storage
                            OPERATOR_TYPE IN ('TableScan', 'ExternalScan')
                            AND OPERATOR_ATTRIBUTES:table_name IS NOT NULL
                    ),
                    PARSED_TABLES AS (
                        -- Step 2: Parse the fully qualified name into its three components for joining
                        SELECT
                            fully_qualified_table_name,
                            -- Use SPLIT_PART to extract DB, Schema, and Table Name
                            SPLIT_PART(fully_qualified_table_name, '.', 1) AS db_name,
                            SPLIT_PART(fully_qualified_table_name, '.', 2) AS schema_name,
                            SPLIT_PART(fully_qualified_table_name, '.', 3) AS table_name
                        FROM
                            BASE_TABLES_LIST
                        WHERE
                        -- Robustness Check: Ensure the string splits into exactly 3 parts
                            ARRAY_SIZE(SPLIT(fully_qualified_table_name, '.')) = 3                            
                    )
                    -- Step 3: Join the list of tables to the Account Usage or Information Schema view
                    SELECT
                        T.TABLE_CATALOG AS database_name,
                        T.TABLE_SCHEMA AS schema_name,
                        T.TABLE_NAME AS table_name,
                        T.ROW_COUNT,
                        T.BYTES,
                        T.CLUSTERING_KEY
                    FROM
                        -- Use SNOWFLAKE.ACCOUNT_USAGE.TABLES for metadata across all databases
                        -- (Recommended if your role has access to ACCOUNT_USAGE)
                        SNOWFLAKE.ACCOUNT_USAGE.TABLES AS T
                    INNER JOIN
                        PARSED_TABLES AS L
                        ON
                            T.TABLE_CATALOG = L.db_name
                            AND T.TABLE_SCHEMA = L.schema_name
                            AND T.TABLE_NAME = L.table_name
                    WHERE
                        T.TABLE_TYPE = 'BASE TABLE' -- Exclude views, materialized views, etc.
                    ORDER BY
                        T.TABLE_CATALOG, T.TABLE_SCHEMA, T.TABLE_NAME
                    """
                    
                    cursor.execute(table_details_query, (query_id,))
                    table_results = cursor.fetchall()
                    
                    # Convert to list of dicts
                    column_names = [desc[0] for desc in cursor.description]
                    table_results = [dict(zip(column_names, row)) for row in table_results]
                    
                    # Process results for this query
                    query_table_details = []
                    for result in table_results:
                        table_info = {
                            'database_name': result['DATABASE_NAME'],
                            'schema_name': result['SCHEMA_NAME'],
                            'table_name': result['TABLE_NAME'],
                            'row_count': result['ROW_COUNT'],
                            'bytes': result['BYTES'],
                            'clustering_key': result['CLUSTERING_KEY'],
                            'size_mb': round(result['BYTES'] / (1024 * 1024), 2) if result['BYTES'] else 0,
                            'fully_qualified_name': f"{result['DATABASE_NAME']}.{result['SCHEMA_NAME']}.{result['TABLE_NAME']}"
                        }
                        query_table_details.append(table_info)
                    
                    all_table_details[query_id] = query_table_details
                    logger.info(f"✅ Found {len(query_table_details)} tables for query {query_id[:8]}...")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Failed to get table details for query {query_id}: {str(e)}")
                    # Store empty details for failed queries
                    all_table_details[query_id] = []
            
            cursor.close()
            connection.close()
            
            execution_time = (time.time() - start_time) * 1000
            logger.info(f"✅ Table object details fetch completed in {execution_time:.2f}ms")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data={"table_details": all_table_details, "query_count": len(query_ids)},
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Table object details fetch failed after {execution_time:.2f}ms: {e}")
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data={"table_details": {}, "query_count": 0},
                error_message=str(e),
                execution_time_ms=execution_time
            )


class ReportGenerationTool:
    """Tool for generating the final optimization report."""
    
    def __init__(self):
        self.name = "report_generation"
    
    def __call__(self, session_id: Optional[str], analyses: List[QueryAnalysisResult], query_tag: Optional[str] = None, start_date: Optional[str] = None, query_id: Optional[str] = None) -> ToolResult:
        """Generate the final optimization report."""
        start_time = time.time()
        
        try:
            logger.info(f"📋 Generating optimization report for {len(analyses)} analyses...")
            
            # Aggregate findings
            total_bottlenecks = []
            total_recommendations = []
            queries_needing_rewrite = 0
            infrastructure_changes = []
            
            for analysis in analyses:
                total_bottlenecks.extend(analysis.bottlenecks)
                total_recommendations.extend(analysis.optimization_recommendations)
                
                if analysis.query_rewrite_needed:
                    queries_needing_rewrite += 1
                
                infrastructure_changes.extend(analysis.infrastructure_changes)
            
            # Calculate summary statistics
            total_execution_time = sum(a.execution_time_seconds for a in analyses)
            
            # Generate appropriate session identifier based on analysis parameters
            if session_id:
                analysis_session_id = session_id
            elif query_tag and start_date:
                analysis_session_id = f"tag_{query_tag}_date_{start_date}"
            elif query_id:
                analysis_session_id = f"query_{query_id}"
            else:
                analysis_session_id = f"analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Create structured optimization report
            session_analysis = SessionAnalysisSummary(
                session_id=analysis_session_id,
                analysis_timestamp=datetime.now().isoformat(),
                total_queries_analyzed=len(analyses),
                total_execution_time_seconds=total_execution_time
            )
            
            summary = AnalysisSummary(
                total_bottlenecks_found=len(total_bottlenecks),
                queries_needing_rewrite=queries_needing_rewrite,
                infrastructure_changes_recommended=len(infrastructure_changes),
                estimated_total_improvement="20-40% performance improvement expected"
            )
            
            aggregated_recommendations = AggregatedRecommendations(
                top_bottlenecks=total_bottlenecks[:10],  # Top 10 bottlenecks
                priority_optimizations=total_recommendations[:15],  # Top 15 recommendations
                infrastructure_changes=infrastructure_changes
            )
            
            optimization_report = OptimizationReport(
                session_analysis=session_analysis,
                summary=summary,
                detailed_analyses=analyses,
                aggregated_recommendations=aggregated_recommendations,
                next_steps=[
                    "Review and prioritize optimization recommendations",
                    "Implement high-priority infrastructure changes",
                    "Test query rewrites in development environment",
                    "Monitor performance improvements after implementation"
                ]
            )
            
            execution_time = (time.time() - start_time) * 1000
            logger.info(f"✅ Optimization report generated in {execution_time:.2f}ms")
            
            return ToolResult(
                success=True,
                tool_name=self.name,
                data=optimization_report.model_dump(),
                execution_time_ms=execution_time
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"❌ Report generation failed after {execution_time:.2f}ms: {e}")
            
            # Create empty optimization report for error case
            empty_session = SessionAnalysisSummary(
                session_id=session_id or "unknown",
                analysis_timestamp=datetime.now().isoformat(),
                total_queries_analyzed=0,
                total_execution_time_seconds=0.0
            )
            
            empty_summary = AnalysisSummary(
                total_bottlenecks_found=0,
                queries_needing_rewrite=0,
                infrastructure_changes_recommended=0,
                estimated_total_improvement="Analysis failed - unable to determine"
            )
            
            empty_recommendations = AggregatedRecommendations()
            
            empty_report = OptimizationReport(
                session_analysis=empty_session,
                summary=empty_summary,
                detailed_analyses=[],
                aggregated_recommendations=empty_recommendations,
                next_steps=["Retry analysis after resolving errors"]
            )
            
            return ToolResult(
                success=False,
                tool_name=self.name,
                data=empty_report.model_dump(),
                error_message=str(e),
                execution_time_ms=execution_time
            )