"""
Streamlit App for Snowflake Performance Agent
Interactive interface for analyzing query performance using a generic Snowflake connection.
"""

import streamlit as st
import sys
import os
import logging
import logging.handlers
import traceback
import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# os.environ["INTERLINKED_CONFIG"]="production" # Removed as per instructions

# Add current directory to path to import the performance agent
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# ============================================================================
# ENHANCED LOGGING CONFIGURATION FOR STREAMLIT UI
# ============================================================================

def setup_ui_logging(log_level=logging.INFO):
    """
    Set up console-only logging for Streamlit UI (Docker-friendly).
    
    Args:
        log_level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    # Create formatter for console
    console_formatter = logging.Formatter(
        '%(asctime)s - UI - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Get the module-specific logger
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()
    
    # === CONSOLE HANDLER ONLY ===
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Log the logging setup completion
    logger.info("=" * 80)
    logger.info("🖥️ SNOWFLAKE PERFORMANCE AGENT UI - STREAMLIT ")
    logger.info("=" * 80)
    logger.info(f"📝 UI Logging configured:")
    logger.info(f"   • Console: {log_level} level (Docker-friendly)")
    logger.info("=" * 80)
    
    return logger

# Configure UI logging with file output
ui_logger = setup_ui_logging()

def check_dependencies():
    """Check if all required dependencies are installed."""
    missing_deps = []
    required_packages = [
        ("langgraph", "langgraph"),
        ("google.generativeai", "google-generativeai"),
        ("snowflake.connector", "snowflake-connector-python")
    ]
    
    for module_name, package_name in required_packages:
        try:
            __import__(module_name)
        except ImportError:
            missing_deps.append(package_name)
    
    return missing_deps

def show_dependency_error(missing_deps):
    """Display dependency error in a clean, concise way."""
    st.error("🚨 Missing Required Dependencies")
    
    # Show missing packages in a compact format
    with st.expander("📦 Missing Packages", expanded=True):
        for dep in missing_deps:
            st.code(f"pip install {dep}", language="bash")
    
    # Quick fix section
    with st.expander("🔧 Quick Fix"):
        st.markdown("**Install all requirements:**")
        st.code("pip install langgraph google-generativeai snowflake-connector-python", language="bash")
        st.markdown("**Or use requirements file:**")
        st.code("pip install -r streamlit_requirements.txt", language="bash")
        st.warning("💡 Restart Streamlit after installation")

# Check dependencies
ui_logger.info("🔍 Checking required dependencies...")
missing_deps = check_dependencies()
if missing_deps:
    ui_logger.error(f"❌ Missing dependencies detected: {missing_deps}")
    show_dependency_error(missing_deps)
    st.stop()
else:
    ui_logger.info("✅ All dependencies are available")

# Import performance agent
ui_logger.info("📦 Importing performance agent...")
try:
    from sf_performance_agent_langgraph import create_langgraph_agent
    from models.data_models import ProgressUpdate
    ui_logger.info("✅ Performance agent imported successfully")
except ImportError as e:
    ui_logger.error(f"❌ Failed to import performance agent: {e}")
    st.error(f"❌ Failed to import performance agent: {e}")
    st.info("💡 Ensure `sf_performance_agent_langgraph.py` and modular components are in the same directory")
    st.stop()

# Configure Streamlit page
ui_logger.info("🎨 Configuring Streamlit page layout...")
st.set_page_config(
    page_title="Snowflake Performance Agent ",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)
ui_logger.info("✅ Streamlit page configuration complete")

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-container {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .query-box {
        background-color: #ffffff;
        border: 1px solid #e1e5e9;
        border-radius: 0.5rem;
        padding: 1rem;
        margin: 1rem 0;
    }
    .recommendation-item {
        padding: 0.5rem 0;
        border-bottom: 1px solid #f1f3f4;
    }
    .recommendation-item:last-child {
        border-bottom: none;
    }
    .section-divider {
        margin: 2rem 0 1rem 0;
        border-top: 2px solid #e1e5e9;
        padding-top: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def create_metric_card(title, value, delta=None):
    """Create a consistent metric display."""
    return st.metric(title, value, delta)

def create_info_section(title, items, icon="•", max_items=None):
    """Create a consistent info section with bullet points."""
    if items:
        if max_items:
            items = items[:max_items]
        st.markdown(f"**{title}:**")
        for item in items:
            st.markdown(f"{icon} {item}")
    else:
        st.info(f"No {title.lower()} available")

def extract_operation_details(description):
    """Extract operation details from bottleneck description."""
    # Extract operation type (e.g., TableScan, HashJoin, Sort, etc.)
    operation_match = re.search(r'(\w+)\s*operator', description, re.IGNORECASE)
    operation_type = operation_match.group(1) if operation_match else "Unknown"
    
    # Extract table names
    table_matches = re.findall(r'\b([A-Z_][A-Z0-9_]*)\s+table', description, re.IGNORECASE)
    if not table_matches:
        # Try to find table names in other patterns
        table_matches = re.findall(r'on\s+([A-Z_][A-Z0-9_]*)', description, re.IGNORECASE)
    
    # Extract row counts
    row_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:million|billion|M|B|rows?)', description, re.IGNORECASE)
    rows_processed = row_match.group(1) if row_match else ""
    
    # Extract data volumes
    volume_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:GB|MB|TB)', description, re.IGNORECASE)
    data_volume = volume_match.group(0) if volume_match else ""
    
    return {
        'operation_type': operation_type,
        'tables': table_matches,
        'rows_processed': rows_processed,
        'data_volume': data_volume
    }

def extract_execution_percentage(impact_text):
    """Extract execution time percentage from impact description."""
    # Look for percentage patterns
    percentage_match = re.search(r'(\d+(?:\.\d+)?)\s*%', impact_text)
    if percentage_match:
        return f"{percentage_match.group(1)}%"
    
    # Look for time fraction patterns
    fraction_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:of|\/)\s*(?:overall|total|execution)', impact_text, re.IGNORECASE)
    if fraction_match:
        return f"{fraction_match.group(1)}%"
    
    return "Unknown"

# ============================================================================
# DATABASE OPERATION CONTEXT ANALYZER (from enhanced app)
# ============================================================================

class DatabaseOperationAnalyzer:
    """Analyze database operations to provide clear context about what's happening."""
    
    @staticmethod
    def analyze_operation_context(description: str, impact: str) -> dict:
        """Analyze operation description to provide clear database context."""
        
        # Identify operation type
        operation_type = DatabaseOperationAnalyzer._identify_operation_type(description)
        
        # Analyze what the operation is doing
        operation_purpose = DatabaseOperationAnalyzer._analyze_operation_purpose(description, operation_type)
        
        # Identify why it's slow
        performance_issue = DatabaseOperationAnalyzer._identify_performance_issue(description, impact)
        
        # Extract quantifiable impact
        quantified_impact = DatabaseOperationAnalyzer._extract_quantified_impact(impact, description)
        
        # Provide business context
        business_context = DatabaseOperationAnalyzer._infer_business_context(description, operation_type)
        
        return {
            'operation_type': operation_type,
            'operation_purpose': operation_purpose,
            'performance_issue': performance_issue,
            'quantified_impact': quantified_impact,
            'business_context': business_context,
            'technical_details': {
                'description': description,
                'impact': impact
            }
        }
    
    @staticmethod
    def analyze_operation_context_enhanced(description: str, impact: str, specific_operation: str = '',
                                         affected_tables: list = None, performance_metrics: dict = None,
                                         root_cause: str = '') -> dict:
        """Enhanced analysis using additional data from improved bottleneck analysis."""
        if affected_tables is None:
            affected_tables = []
        if performance_metrics is None:
            performance_metrics = {}
        
        # Use specific operation details if available, otherwise fallback to basic analysis
        if specific_operation:
            operation_type = DatabaseOperationAnalyzer._extract_operation_type_from_specific(specific_operation)
        else:
            operation_type = DatabaseOperationAnalyzer._identify_operation_type(description)
        
        # Analyze what the operation is doing with enhanced data
        operation_purpose = DatabaseOperationAnalyzer._analyze_operation_purpose_enhanced(
            description, operation_type, specific_operation, affected_tables, performance_metrics
        )
        
        # Identify performance issues with enhanced context
        performance_issue = DatabaseOperationAnalyzer._identify_performance_issue_enhanced(
            description, impact, root_cause, performance_metrics
        )
        
        # Extract quantified impact from enhanced metrics
        quantified_impact = DatabaseOperationAnalyzer._process_performance_metrics(performance_metrics, impact, description)
        
        # Provide business context using actual table names
        business_context = DatabaseOperationAnalyzer._infer_business_context_from_tables(affected_tables, operation_type)
        
        return {
            'operation_type': operation_type,
            'operation_purpose': operation_purpose,
            'performance_issue': performance_issue,
            'quantified_impact': quantified_impact,
            'business_context': business_context,
            'technical_details': {
                'description': description,
                'impact': impact,
                'specific_operation': specific_operation,
                'affected_tables': affected_tables,
                'performance_metrics': performance_metrics,
                'root_cause': root_cause
            }
        }
    
    @staticmethod
    def _extract_operation_type_from_specific(specific_operation: str) -> str:
        """Extract operation type from specific operation details."""
        if not specific_operation:
            return 'Unknown Operation'
        
        operation_lower = specific_operation.lower()
        
        if 'tablescan' in operation_lower:
            return 'TableScan'
        elif 'hashjoin' in operation_lower:
            return 'HashJoin'
        elif 'sort' in operation_lower:
            return 'Sort'
        elif 'aggregate' in operation_lower or 'groupby' in operation_lower:
            return 'Aggregation'
        elif 'filter' in operation_lower:
            return 'Filter'
        elif 'join' in operation_lower:
            return 'Join'
        else:
            return specific_operation.title()
    
    @staticmethod
    def _analyze_operation_purpose_enhanced(description: str, operation_type: str, specific_operation: str = '',
                                          affected_tables: list = None, performance_metrics: dict = None) -> str:
        """Enhanced analysis of what the operation is accomplishing."""
        if affected_tables is None:
            affected_tables = []
        if performance_metrics is None:
            performance_metrics = {}
        
        operation_lower = operation_type.lower()
        
        # Use actual table names from enhanced data
        table_names = affected_tables if affected_tables else []
        if not table_names:
            # Fallback to extracting from description
            table_names = re.findall(r'\b([A-Z_][A-Z0-9_]{2,})', description)
        
        # Format table names for display
        table_display = ', '.join(table_names[:3]) if table_names else 'table(s)'
        if len(table_names) > 3:
            table_display += f' (and {len(table_names) - 3} more)'
        
        # Get performance metrics for context
        rows_processed = performance_metrics.get('rows_processed', '')
        data_volume = performance_metrics.get('data_volume', '')
        
        if 'tablescan' in operation_lower:
            purpose = f"TableScan operator scanning {table_display}"
            if rows_processed and rows_processed != 'Unknown':
                purpose += f" processed {rows_processed} rows"
            if data_volume and data_volume != 'Unknown':
                purpose += f" ({data_volume})"
            return purpose
        
        elif 'hashjoin' in operation_lower or 'join' in operation_lower:
            purpose = f"HashJoin operator combining data from {table_display}"
            if rows_processed and rows_processed != 'Unknown':
                purpose += f" processed {rows_processed} rows"
            return purpose
        
        elif 'sort' in operation_lower:
            data_volume_val = performance_metrics.get('data_volume', '')
            if data_volume_val and data_volume_val != 'Unknown':
                return f"Sorting {data_volume_val} of data for ordering or join preparation"
            elif rows_processed and rows_processed != 'Unknown':
                return f"Sorting {rows_processed} rows for ordering or join preparation"
            else:
                return "Sorting data for ordering or join preparation"
        
        elif 'aggregate' in operation_lower:
            if table_display != 'table(s)':
                return f"Aggregating data from {table_display} (COUNT, SUM, AVG operations)"
            else:
                return "Calculating summary statistics (COUNT, SUM, AVG, etc.)"
        
        elif 'filter' in operation_lower:
            if table_display != 'table(s)':
                return f"Filtering data from {table_display} using WHERE conditions"
            else:
                return "Applying WHERE conditions to filter data"
        
        else:
            if table_display != 'table(s)':
                return f"{operation_type} operation on {table_display}"
            else:
                return f"Performing {operation_type} operation"
    
    @staticmethod
    def _identify_performance_issue_enhanced(description: str, impact: str, root_cause: str = '',
                                           performance_metrics: dict = None) -> str:
        """Enhanced performance issue identification using root cause and metrics."""
        if performance_metrics is None:
            performance_metrics = {}
        
        # Use root cause if available
        if root_cause:
            return root_cause
        
        combined_text = (description + " " + impact).lower()
        
        # Check for specific performance issues with metrics
        memory_usage = performance_metrics.get('memory_usage', '')
        spill_data = performance_metrics.get('spill_to_disk', '')
        
        if spill_data and spill_data != 'Unknown':
            return f"Memory overflow causing {spill_data} to spill to disk"
        elif 'spill' in combined_text:
            return "Memory overflow causing data to spill to disk"
        elif memory_usage and 'gb' in memory_usage.lower():
            return f"High memory usage ({memory_usage}) causing performance degradation"
        elif 'full table scan' in combined_text or 'table scan' in combined_text:
            return "Full table scan without efficient indexing/clustering"
        elif 'large' in combined_text and ('join' in combined_text or 'hash' in combined_text):
            return "Large data volume in join operation causing memory pressure"
        elif 'partition' in combined_text and 'scan' in combined_text:
            return "Inefficient partition pruning leading to excessive data scanning"
        elif 'network' in combined_text:
            return "Network communication overhead between compute nodes"
        elif 'disk' in combined_text or 'i/o' in combined_text:
            return "Disk I/O bottleneck from data reading/writing"
        else:
            return "High resource consumption in database operation"
    
    @staticmethod
    def _process_performance_metrics(performance_metrics: dict, impact: str, description: str) -> dict:
        """Process enhanced performance metrics into quantified impact."""
        if not performance_metrics:
            performance_metrics = {}
        
        # Extract from enhanced metrics first, then fallback to text parsing
        execution_time_pct = performance_metrics.get('execution_time_percentage', 'Unknown')
        data_volume = performance_metrics.get('data_volume', 'Unknown')
        rows_affected = performance_metrics.get('rows_processed', 'Unknown')
        memory_usage = performance_metrics.get('memory_usage', 'Unknown')
        
        # Fallback to text parsing if metrics not available
        if execution_time_pct == 'Unknown':
            combined_text = impact + " " + description
            time_match = re.search(r'(\d+(?:\.\d+)?)\s*%', combined_text)
            execution_time_pct = time_match.group(1) if time_match else "Unknown"
        
        if data_volume == 'Unknown':
            combined_text = impact + " " + description
            volume_match = re.search(r'(\d+(?:\.\d+)?)\s*(GB|MB|TB)', combined_text, re.IGNORECASE)
            data_volume = volume_match.group(0) if volume_match else "Unknown"
        
        if rows_affected == 'Unknown':
            combined_text = impact + " " + description
            row_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:billion|million|M|B|rows?)', combined_text, re.IGNORECASE)
            rows_affected = row_match.group(1) if row_match else "Unknown"
        
        return {
            'execution_time_percentage': execution_time_pct,
            'data_volume_processed': data_volume,
            'rows_affected': rows_affected,
            'memory_usage': memory_usage
        }
    
    @staticmethod
    def _infer_business_context_from_tables(affected_tables: list, operation_type: str) -> str:
        """Infer business context using actual table names from enhanced data."""
        if not affected_tables:
            # Fallback to operation type
            if operation_type in ['HashJoin', 'Join']:
                return 'Data integration and relationship analysis'
            elif operation_type == 'Aggregation':
                return 'Business metrics calculation and reporting'
            elif operation_type == 'TableScan':
                return 'Data retrieval for analysis or reporting'
            else:
                return 'Business data processing'
        
        # Return generic context based on table count
        if len(affected_tables) > 1:
            return f'Multi-table data processing involving {", ".join(affected_tables[:3])}'
        else:
            return f'Data processing on {affected_tables[0]} table'
    
    @staticmethod
    def _identify_operation_type(description: str) -> str:
        """Identify the type of database operation."""
        desc_lower = description.lower()
        
        if 'tablescan' in desc_lower or 'table scan' in desc_lower:
            return 'Table Scan'
        elif 'hashjoin' in desc_lower or 'hash join' in desc_lower:
            return 'Hash Join'
        elif 'sort' in desc_lower and 'operator' in desc_lower:
            return 'Sort Operation'
        elif 'aggregate' in desc_lower or 'groupby' in desc_lower:
            return 'Aggregation'
        elif 'filter' in desc_lower:
            return 'Filtering'
        elif 'join' in desc_lower:
            return 'Join Operation'
        else:
            return 'Unknown Operation'
    
    @staticmethod
    def _analyze_operation_purpose(description: str, operation_type: str) -> str:
        """Analyze what the operation is trying to accomplish."""
        
        if operation_type == 'Table Scan':
            # Extract table names
            tables = re.findall(r'\b([A-Z_][A-Z0-9_]{2,})', description)
            if tables:
                return f"Reading data from {', '.join(tables[:3])} table(s)"
            else:
                return "Reading data from table(s)"
        
        elif operation_type == 'Hash Join':
            return "Combining data from multiple tables using hash join algorithm"
        
        elif operation_type == 'Sort Operation':
            return "Sorting data for ordering or join preparation"
        
        elif operation_type == 'Aggregation':
            return "Calculating summary statistics (COUNT, SUM, AVG, etc.)"
        
        elif operation_type == 'Filtering':
            return "Applying WHERE conditions to filter data"
        
        else:
            return "Performing database operation"
    
    @staticmethod
    def _identify_performance_issue(description: str, impact: str) -> str:
        """Identify why the operation is performing poorly."""
        
        combined_text = (description + " " + impact).lower()
        
        if 'spill' in combined_text:
            return "Memory overflow causing data to spill to disk"
        elif 'full table scan' in combined_text or 'table scan' in combined_text:
            return "Full table scan without efficient indexing/clustering"
        elif 'large' in combined_text and ('join' in combined_text or 'hash' in combined_text):
            return "Large data volume in join operation causing memory pressure"
        elif 'partition' in combined_text and 'scan' in combined_text:
            return "Inefficient partition pruning leading to excessive data scanning"
        elif 'network' in combined_text:
            return "Network communication overhead between compute nodes"
        elif 'disk' in combined_text or 'i/o' in combined_text:
            return "Disk I/O bottleneck from data reading/writing"
        else:
            return "High resource consumption in database operation"
    
    @staticmethod
    def _extract_quantified_impact(impact: str, description: str) -> dict:
        """Extract quantifiable impact metrics."""
        
        combined_text = impact + " " + description
        
        # Extract execution time percentage
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*%', combined_text)
        execution_time_pct = time_match.group(1) if time_match else "Unknown"
        
        # Extract data volume
        volume_match = re.search(r'(\d+(?:\.\d+)?)\s*(GB|MB|TB)', combined_text, re.IGNORECASE)
        data_volume = volume_match.group(0) if volume_match else "Unknown"
        
        # Extract row count
        row_match = re.search(r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:billion|million|M|B|rows?)', combined_text, re.IGNORECASE)
        rows_affected = row_match.group(1) if row_match else "Unknown"
        
        # Extract memory usage
        memory_match = re.search(r'(\d+(?:\.\d+)?)\s*GB.*memory', combined_text, re.IGNORECASE)
        memory_usage = memory_match.group(0) if memory_match else "Unknown"
        
        return {
            'execution_time_percentage': execution_time_pct,
            'data_volume_processed': data_volume,
            'rows_affected': rows_affected,
            'memory_usage': memory_usage
        }
    
    @staticmethod
    def _infer_business_context(description: str, operation_type: str) -> str:
        """Infer business context of the operation."""
        
        # Default based on operation type
        if operation_type in ['Hash Join', 'Join Operation']:
            return 'Data integration and relationship analysis'
        elif operation_type == 'Aggregation':
            return 'Business metrics calculation and reporting'
        elif operation_type == 'Table Scan':
            return 'Data retrieval for analysis or reporting'
        else:
            return 'Business data processing'

def extract_operation_contexts_from_bottlenecks(bottlenecks):
    """Extract operation contexts from enhanced bottleneck analysis for Database Operation Context section."""
    contexts = []
    
    for bottleneck in bottlenecks:
        if hasattr(bottleneck, 'description'):
            desc = bottleneck.description
            impact = getattr(bottleneck, 'impact', '')
            # Get enhanced fields if available
            specific_operation = getattr(bottleneck, 'specific_operation', '')
            affected_tables = getattr(bottleneck, 'affected_tables', [])
            performance_metrics = getattr(bottleneck, 'performance_metrics', {})
            root_cause = getattr(bottleneck, 'root_cause', '')
        elif isinstance(bottleneck, dict):
            desc = bottleneck.get('description', '')
            impact = bottleneck.get('impact', '')
            # Get enhanced fields if available
            specific_operation = bottleneck.get('specific_operation', '')
            affected_tables = bottleneck.get('affected_tables', [])
            performance_metrics = bottleneck.get('performance_metrics', {})
            root_cause = bottleneck.get('root_cause', '')
        else:
            continue
        
        # Determine operation context using enhanced data
        operation_context = DatabaseOperationAnalyzer.analyze_operation_context_enhanced(
            desc, impact, specific_operation, affected_tables, performance_metrics, root_cause
        )
        if operation_context:
            contexts.append(operation_context)
    
    return contexts

def create_operation_context_section(operation_contexts):
    """Create database operation context section as a table."""
    
    st.markdown("### 🗄️ Database Operation Context")
    st.markdown("*Understanding what database operations are happening and why they're slow*")
    
    if not operation_contexts:
        st.info("No detailed operation context available")
        return
    
    # Prepare data for the table
    table_data = []
    
    for i, context in enumerate(operation_contexts):
        impact = context['quantified_impact']
        
        # Get time impact
        time_impact = impact['execution_time_percentage']
        if time_impact != "Unknown":
            time_impact = f"{time_impact}%"
        
        # Get data volume
        data_volume = impact['data_volume_processed']
        if data_volume == "Unknown":
            data_volume = "N/A"
        
        # Get rows affected
        rows_affected = impact['rows_affected']
        if rows_affected == "Unknown":
            rows_affected = "N/A"
        
        table_data.append({
            'Operation': context['operation_type'],
            'What is happening': context['operation_purpose'],
            'Why it\'s slow': context['performance_issue'],
            'Time Impact': time_impact,
            'Data Volume': data_volume,
            'Rows Affected': rows_affected
        })
    
    # Create DataFrame and display as table
    if table_data:
        df = pd.DataFrame(table_data)
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Operation": st.column_config.TextColumn("Operation Type", width="small"),
                "What is happening": st.column_config.TextColumn("What is happening", width="medium"),
                "Why it's slow": st.column_config.TextColumn("Why it's slow", width="medium"),
                "Time Impact": st.column_config.TextColumn("Time Impact", width="small"),
                "Data Volume": st.column_config.TextColumn("Data Volume", width="small"),
                "Rows Affected": st.column_config.TextColumn("Rows Affected", width="small")
            }
        )


def create_query_structure_analysis_section(opt_recommendations, original_query):
    """Create a section for query structure analysis (AI-powered recommendations only)."""
    if not opt_recommendations:
        st.info("No structural recommendations available")
        return
    
    # Group AI-generated recommendations by type
    recommendation_groups = {}
    for rec in opt_recommendations:
        if hasattr(rec, 'type'):  # Pydantic object
            rec_type = rec.type
            rec_desc = rec.description
            rec_improvement = getattr(rec, 'expected_improvement', 'Unknown improvement')
        elif isinstance(rec, dict):  # Dictionary
            rec_type = rec.get('type', 'general')
            rec_desc = rec.get('description', 'No description')
            rec_improvement = rec.get('expected_improvement', 'Unknown improvement')
        else:
            rec_type = 'general'
            rec_desc = str(rec)
            rec_improvement = 'Unknown improvement'
        
        if rec_type not in recommendation_groups:
            recommendation_groups[rec_type] = []
        recommendation_groups[rec_type].append({
            'description': rec_desc,
            'improvement': rec_improvement
        })
    
    # Display AI-generated recommendations grouped by type
    st.markdown("**AI-Generated Optimization Recommendations by Type:**")
    for rec_type, recs in recommendation_groups.items():
        with st.expander(f"🤖 {rec_type.replace('_', ' ').title()} ({len(recs)} recommendations)", expanded=True):
            for i, rec in enumerate(recs, 1):
                st.markdown(f"**{i}.** {rec['description']}")
                st.markdown(f"   *Expected improvement: {rec['improvement']}*")
                if i < len(recs):
                    st.markdown("---")

def display_performance_analysis_results(analysis, query_id=None, full_report=None):
    """Display the raw performance analysis results with better formatting."""
    
    # Try multiple sources for performance analysis data
    bottlenecks = analysis.get("bottlenecks", [])
    
    # CRITICAL FIX: If no bottlenecks in analysis, try to get from performance_analyses in full_report
    if not bottlenecks and full_report and query_id:
        # Try to extract from the detailed_analyses in the full report
        detailed_analyses = full_report.get("detailed_analyses", [])
        current_analysis = None
        for detailed_analysis in detailed_analyses:
            if detailed_analysis.get("query_id") == query_id:
                current_analysis = detailed_analysis
                break
        
        if current_analysis:
            bottlenecks = current_analysis.get("bottlenecks", [])
    
    # ADDITIONAL FIX: Try to extract performance analysis data from the analysis itself
    # Check if there's performance analysis data that needs to be converted to bottlenecks
    if not bottlenecks:
        # Look for performance_analysis field which contains the AI analysis results
        perf_analysis_data = analysis.get("performance_analysis", [])
        if perf_analysis_data:
            # Convert performance_analysis items to bottleneck format for display
            bottlenecks = []
            for item in perf_analysis_data:
                if isinstance(item, dict):
                    # Create a bottleneck-like structure from performance analysis
                    # CRITICAL FIX: Preserve all fields from LLM response, including root_cause and operator_details
                    bottleneck = {
                        'type': item.get('type', 'performance_issue'),
                        'description': item.get('description', 'Performance issue detected'),
                        'severity': item.get('severity', 'medium'),
                        'impact': item.get('impact', ''),
                        'root_cause': item.get('root_cause', None),  # Keep None to distinguish from empty string
                        'affected_tables': item.get('affected_tables', []),
                        'resource_impact': item.get('resource_impact', {}),
                        'performance_metrics': item.get('performance_metrics', {}),
                        'operator_details': item.get('operator_details', None)  # Keep None to distinguish from empty string
                    }
                    bottlenecks.append(bottleneck)
    
    if not bottlenecks:
        st.info("No detailed performance analysis results available")
        return
    
    st.markdown("### 📊 Performance Analysis Results")
    st.markdown("*Detailed AI-powered analysis of query performance characteristics*")
    
    # Convert bottlenecks to performance analysis format for consistent display
    performance_items = []
    for bottleneck in bottlenecks:
        if hasattr(bottleneck, 'type'):  # Pydantic object
            item = {
                'type': bottleneck.type,
                'description': bottleneck.description,
                'severity': bottleneck.severity,
                'impact': getattr(bottleneck, 'impact', ''),
                'root_cause': getattr(bottleneck, 'root_cause', None),
                'affected_tables': getattr(bottleneck, 'affected_tables', []),
                'resource_impact': getattr(bottleneck, 'resource_impact', {}),
                'performance_metrics': getattr(bottleneck, 'performance_metrics', {}),
                'operator_details': getattr(bottleneck, 'operator_details', None)
            }
        elif isinstance(bottleneck, dict):  # Dictionary
            item = {
                'type': bottleneck.get('type', 'unknown'),
                'description': bottleneck.get('description', ''),
                'severity': bottleneck.get('severity', 'unknown'),
                'impact': bottleneck.get('impact', ''),
                'root_cause': bottleneck.get('root_cause', None),
                'affected_tables': bottleneck.get('affected_tables', []),
                'resource_impact': bottleneck.get('resource_impact', {}),
                'performance_metrics': bottleneck.get('performance_metrics', {}),
                'operator_details': bottleneck.get('operator_details', None)
            }
        else:
            continue
        
        performance_items.append(item)
    
    if performance_items:
        st.markdown("#### 🔍 Performance Issues Identified")
        
        # Prepare data for the performance issues table
        table_data = []
        
        for i, item in enumerate(performance_items, 1):
            # Get severity with icon
            severity = item.get('severity', 'unknown')
            severity_colors = {
                'critical': '🔴',
                'high': '🟠',
                'medium': '🟡',
                'medium-high': '🟠',
                'low': '🟢'
            }
            severity_icon = severity_colors.get(severity.lower(), '⚫')
            severity_display = f"{severity_icon} {severity.title()}"
            
            # Prepare table row with proper handling of None/empty values
            # CRITICAL FIX: Only use fallback if the field is None or truly empty
            root_cause = item.get('root_cause')
            if root_cause is None or root_cause == '':
                root_cause = 'Not specified'
            
            table_data.append({
                'Issue': item.get('type', 'Unknown').replace('_', ' ').title(),
                'Description': item.get('description', 'No description available'),
                'Root Cause': root_cause,
                'Impact': item.get('impact', 'No impact information available'),
                'Severity': severity_display
            })
        
        # Create DataFrame and display as table
        if table_data:
            df = pd.DataFrame(table_data)
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Issue": st.column_config.TextColumn("Issue", width="small"),
                    "Description": st.column_config.TextColumn("Description", width="large"),
                    "Root Cause": st.column_config.TextColumn("Root Cause", width="medium"),
                    "Operator Details": st.column_config.TextColumn("Operator Details", width="medium"),
                    "Impact": st.column_config.TextColumn("Impact", width="large"),
                    "Severity": st.column_config.TextColumn("Severity", width="small")
                }
            )
    
    # Try to extract query characteristics and performance metrics from the analysis
    # These might be in the estimated_performance_gain or other fields
    estimated_gain = analysis.get('estimated_performance_gain', '')
    if estimated_gain:
        st.markdown("#### 🎯 Performance Gain Potential")
        st.success(f"📊 {estimated_gain}")


# analyze_query_structural_patterns function removed - replaced with AI-based analysis

def show_welcome_screen():
    """Display welcome screen with tool information."""
    st.info("✨ Enter a Session ID, Query Tag with Start Date, OR Query ID in the sidebar to begin analysis")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 🎯 What This Tool Does")
        features = [
            "**Fetches** slow queries from Snowflake session history",
            "**Analyzes** query execution profiles using AI",
            "**Identifies** performance bottlenecks and optimization opportunities",
            "**Generates** optimized query versions with explanations",
            "**Provides** infrastructure recommendations",
            "**Uses** generic user/password based Snowflake connection"
        ]
        for feature in features:
            st.markdown(f"• {feature}")
        
        # Add LangGraph specific features
        st.markdown("### 🔀 LangGraph Workflow Features")
        langgraph_features = [
            "**Deterministic** workflow orchestration for reliable execution",
            "**Sequential** step-by-step analysis with conditional transitions",
            "**Reduced** LLM calls through workflow optimization",
            "**Consistent** and repeatable analysis results"
        ]
        for feature in langgraph_features:
            st.markdown(f"• {feature}")
    
    with col2:
        st.markdown("### 📊 Sample Metrics")
        st.metric("Queries Analyzed", "5")
        st.metric("Avg Improvement", "40%")
        st.metric("Time Saved", "2.3 min")

def run_analysis_with_progress(session_id=None, query_tag=None, start_date=None, query_id=None):
    """Run analysis with real-time progress tracking."""
    # Create progress containers
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Create empty containers for clean overwriting
    tool_status_header = st.empty()
    tool_status_grid = st.empty()
    
    # Track tool execution status
    tool_status = {}
    
    # Tool name mapping for display
    tool_display_names = {
        "query_history": "📊 Query History Fetch",
        "query_profiling": "🔍 Query Profiling",
        "query_object_details": "📋 Table Object Details",
        "operator_stats_analysis": "📈 Operator Analysis",
        "query_performance_analysis": "🎯 Performance Analysis",
        "optimized_query_generation": "🔧 Query Optimization",
        "query_semantic_evaluation": "✅ Semantic Validation",
        "report_generation": "📋 Report Generation"
    }
    
    def update_tool_status_display():
        """Update the tool status display by overwriting the container."""
        with tool_status_header:
            st.markdown("### 🔧 LangGraph Workflow Execution Status ")
        
        with tool_status_grid:
            # Create columns for tool status
            cols = st.columns(4)
            col_idx = 0
            
            # Define the expected tool order for consistent display
            expected_tools = [
                "query_history",
                "query_profiling",
                "query_object_details",
                "operator_stats_analysis",
                "query_performance_analysis",
                "optimized_query_generation",
                "query_semantic_evaluation",
                "report_generation"
            ]
            
            # Display tools in order, showing pending state if not yet started
            for tool_name in expected_tools:
                display_name = tool_display_names.get(tool_name, tool_name)
                status_info = tool_status.get(tool_name, {"status": "pending", "message": "", "execution_time": None})
                
                with cols[col_idx % 4]:
                    # Status indicator
                    if status_info["status"] == "completed":
                        # Check for warnings (partial AI failures)
                        if status_info.get("warnings"):
                            st.warning(f"⚠️ {display_name}")
                            if status_info["execution_time"]:
                                st.caption(f"⏱️ {status_info['execution_time']/1000:.2f}s")
                            for warning in status_info["warnings"]:
                                st.caption(warning)
                        else:
                            st.success(f"✅ {display_name}")
                            if status_info["execution_time"]:
                                st.caption(f"⏱️ {status_info['execution_time']/1000:.2f}s")
                    elif status_info["status"] == "failed":
                        st.error(f"❌ {display_name}")
                        if status_info["execution_time"]:
                            st.caption(f"⏱️ {status_info['execution_time']/1000:.2f}s")
                    elif status_info["status"] == "running":
                        st.info(f"⚙️ {display_name}")
                        st.caption("Running...")
                    elif status_info["status"] == "starting":
                        st.warning(f"🔄 {display_name}")
                        st.caption("Starting...")
                    else:
                        st.info(f"⏳ {display_name}")
                        st.caption("Pending...")
                
                col_idx += 1
    
    def progress_callback(update: ProgressUpdate):
        """Callback function to handle progress updates from the agent."""
        tool_name = update.tool_name
        
        # Update tool status tracking
        if tool_name not in tool_status:
            tool_status[tool_name] = {"status": "pending", "message": "", "execution_time": None, "warnings": []}
        
        tool_status[tool_name].update({
            "status": update.status,
            "message": update.message,
            "execution_time": update.execution_time_ms
        })
        
        # Check for AI failure warnings in the message
        if "Some" in update.message and "AI calls failed" in update.message:
            tool_status[tool_name]["warnings"].append("⚠️ Partial AI failures occurred")
        
        # Update main progress bar based on tool completion
        completed_tools = sum(1 for status in tool_status.values() if status["status"] in ["completed", "failed"])
        total_tools = 8  # Expected number of tools (removed snowflake_connection)
        progress_percentage = min((completed_tools / total_tools) * 100, 100)
        
        # Update progress bar
        progress_bar.progress(int(progress_percentage))
        
        # Update main status text (overwrite previous)
        if update.status == "starting":
            status_text.info(f"🔄 {update.message}")
        elif update.status == "running":
            status_text.info(f"⚙️ {update.message}")
        elif update.status == "completed":
            # Check for warnings in the message for partial AI failures
            if "Some" in update.message and "AI calls failed" in update.message:
                status_text.warning(f"⚠️ {update.message} (Partial AI failures detected)")
            else:
                status_text.success(f"✅ {update.message}")
        elif update.status == "failed":
            status_text.error(f"❌ {update.message}")
        
        # Update tool status display (clean overwrite)
        update_tool_status_display()
    
    # Create agent with progress callback
    ui_logger.info("🤖 Creating LangGraph agent...")
    agent = create_langgraph_agent(progress_callback=progress_callback)
    ui_logger.info("✅ LangGraph agent created successfully")
    
    try:
        # Run analysis with real-time progress tracking
        if session_id:
            # Session ID method
            report = agent.analyze_session_performance(session_id, progress_callback=progress_callback)
        elif query_id:
            # Query ID method
            report = agent.analyze_session_performance(None, None, None, query_id=query_id, progress_callback=progress_callback)
        else:
            # Query tag method
            report = agent.analyze_session_performance(None, query_tag=query_tag, start_date=start_date, progress_callback=progress_callback)
        
        # Final status update
        progress_bar.progress(100)
        status_text.success("🎉 LangGraph analysis completed successfully!")
        
        # Clear progress after 2 seconds to show completion
        import time
        time.sleep(2)
        progress_bar.empty()
        status_text.empty()
        tool_status_header.empty()
        tool_status_grid.empty()
        
        return report
        
    except Exception as e:
        progress_bar.empty()
        status_text.error(f"❌ Analysis failed: {str(e)}")
        tool_status_header.empty()
        tool_status_grid.empty()
        raise e

def main():
    """Main Streamlit application."""
    # os.environ["INTERLINKED_CONFIG"]="production" # Removed as per instructions
    ui_logger.info("🖥️ Streamlit UI application started")
    ui_logger.info(f"📍 Current working directory: {os.getcwd()}")
    
    # Header with better spacing
    st.title("⚡ Snowflake Performance Agent ")
    st.markdown("*AI-powered snowflake query performance analysis and optimization*")
    st.divider()
    
    # Sidebar configuration
    with st.sidebar:
        # st.header("🔧 Configuration")
        
        # Add LangGraph workflow info
        # with st.expander("🔀 LangGraph Workflow", expanded=False):
        #     st.markdown("**Workflow Features:**")
        #     st.markdown("• Deterministic execution order")
        #     st.markdown("• Conditional state transitions")
        #     st.markdown("• Reduced LLM API calls")
        #     st.markdown("• Consistent analysis results")
        
        # Additional features can be added here if needed
        
        # Method selection
        analysis_method = st.radio(
            "Analysis Method",
            ["Session ID", "Query Tag", "Query ID"],
            help="Choose how to identify queries for analysis"
        )
        
        # Log method selection changes (using session state to avoid repeated logs)
        if "last_method" not in st.session_state:
            st.session_state.last_method = None
        
        if st.session_state.last_method != analysis_method:
            ui_logger.info(f"📋 User selected analysis method: {analysis_method}")
            st.session_state.last_method = analysis_method
        
        session_id = None
        query_tag = None
        start_date = None
        query_id = None
        ready_to_analyze = False
        
        if analysis_method == "Session ID":
            session_id = st.text_input(
                "Session ID",
                placeholder="e.g., 12345678",
                help="Enter the Snowflake session ID to analyze"
            )
            ready_to_analyze = bool(session_id)
        elif analysis_method == "Query ID":
            query_id = st.text_input(
                "Query ID",
                placeholder="e.g., 01abc123-4567-8901-2345-6789abcdef01",
                help="Enter the specific Snowflake query ID to analyze"
            )
            ready_to_analyze = bool(query_id)
        else:
            st.markdown("**Query Tag Method**")
            query_tag = st.text_input(
                "Query Tag (Text)",
                placeholder="e.g., MY_ANALYSIS_TAG",
                help="Enter the query tag to filter queries"
            )
            start_date = st.date_input(
                "Start Date",
                value=None,
                help="Select the start date to filter queries from"
            )
            ready_to_analyze = bool(query_tag and start_date)

        col1, col2, col3 = st.columns([1,2,1])   # Middle column twice as big
        with col2:
            analyze_button = st.button("Analyze",
                type="primary",
                disabled=not ready_to_analyze
            )

        # Alternative button style - can be used instead of the centered button above
        # analyze_button = st.button(
        #     "🚀 Start LangGraph Analysis",
        #     type="primary",
        #     disabled=not ready_to_analyze,
        #     width='stretch'
        # )
        
        if ready_to_analyze:
            if analysis_method == "Session ID":
                st.success(f"✅ Ready to analyze session: {session_id}")
            elif analysis_method == "Query ID":
                st.success(f"✅ Ready to analyze query: {query_id}")
            else:
                st.success(f"✅ Ready to analyze queries with tag: {query_tag} from {start_date}")
        else:
            if analysis_method == "Session ID":
                st.info("Enter session ID above")
            elif analysis_method == "Query ID":
                st.info("Enter query ID above")
            else:
                st.info("Enter query tag and start date above")
    
    # Main content
    if not ready_to_analyze:
        ui_logger.debug("📖 Displaying welcome screen - no analysis parameters provided")
        show_welcome_screen()
        return
    
    if analyze_button:
        # Log user interaction and analysis initiation
        if analysis_method == "Session ID":
            ui_logger.info(f"🎯 User initiated analysis for Session ID: {session_id}")
        elif analysis_method == "Query ID":
            ui_logger.info(f"🎯 User initiated analysis for Query ID: {query_id}")
        else:
            ui_logger.info(f"🎯 User initiated analysis for Query Tag: {query_tag} from {start_date}")
        
        try:
            ui_logger.info("🚀 Starting LangGraph analysis workflow")
            with st.spinner("Running LangGraph analysis..."):
                if analysis_method == "Session ID":
                    report = run_analysis_with_progress(session_id=session_id)
                elif analysis_method == "Query ID":
                    report = run_analysis_with_progress(query_id=query_id)
                else:
                    report = run_analysis_with_progress(query_tag=query_tag, start_date=start_date)
            
            # Handle different report types
            if "error" in report:
                ui_logger.error(f"❌ Analysis failed: {report['error']}")
                st.error(f"❌ Analysis failed: {report['error']}")
                if "errors" in report:
                    ui_logger.error(f"📋 Additional errors: {report['errors']}")
                    with st.expander("Error Details"):
                        for error in report["errors"]:
                            st.text(error)
                return
            elif "info" in report and report["info"] == "0 queries found":
                # Handle the case when 0 slow queries were found - this is a valid result, not an error
                ui_logger.info("ℹ️ No slow queries found - displaying informational message")
                display_zero_queries_result(report)
                return
            
            # Check for AI failures in tool results and display warnings
            ai_failure_warnings = []
            if "tool_results" in report:
                for tool_result in report.get("tool_results", []):
                    if isinstance(tool_result, dict) and tool_result.get("success") and "ai_failures" in tool_result.get("data", {}):
                        tool_name = tool_result.get("tool_name", "Unknown Tool")
                        ai_failures = tool_result["data"]["ai_failures"]
                        ai_failure_warnings.extend([f"🔧 {tool_name}: {failure}" for failure in ai_failures])
            
            # Log AI failure warnings
            if ai_failure_warnings:
                ui_logger.warning(f"⚠️ AI analysis warnings detected: {len(ai_failure_warnings)} issues")
                for warning in ai_failure_warnings:
                    ui_logger.warning(f"   • {warning}")
                
                with st.expander("⚠️ AI Call Warnings", expanded=True):
                    st.warning("Some AI analysis calls failed but the process continued with partial results:")
                    for warning in ai_failure_warnings:
                        st.write(f"• {warning}")
            
            ui_logger.info("✅ Analysis completed successfully - displaying results")
            display_results(report)
            
        except Exception as e:
            ui_logger.error(f"❌ Critical analysis failure: {str(e)}")
            ui_logger.error(f"📋 Full traceback: {traceback.format_exc()}")
            st.error(f"❌ Analysis failed: {str(e)}")
            with st.expander("Error Details"):
                st.code(traceback.format_exc())

def display_zero_queries_result(report):
    """Display informational message when 0 slow queries are found."""
    
    # Extract session info for display
    session_analysis = report.get("session_analysis", {})
    criteria_used = report.get("criteria_used", {})
    
    # Success message with icon
    st.success("✅ Analysis completed successfully!")
    
    # Main informational message
    st.info("🎉 **No slow queries found for optimization**")
    
    # Details section
    st.markdown("## 📊 Analysis Results")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_metric_card("Slow Queries Found", "0")
    
    with col2:
        create_metric_card("Analysis Status", "Complete")
    
    with col3:
        analysis_timestamp = session_analysis.get("analysis_timestamp", "")
        if analysis_timestamp:
            # Parse and format timestamp
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(analysis_timestamp.replace('Z', '+00:00'))
                formatted_time = dt.strftime("%H:%M:%S")
            except:
                formatted_time = analysis_timestamp.split('T')[1][:8] if 'T' in analysis_timestamp else "Unknown"
        else:
            formatted_time = "Unknown"
        create_metric_card("Analysis Time", formatted_time)
    
    with col4:
        session_id = session_analysis.get("session_id", "Unknown")
        if len(session_id) > 15:
            display_session = f"{session_id[:12]}..."
        else:
            display_session = session_id
        create_metric_card("Session ID", display_session)
    
    st.divider()
    
    # Explanation section
    st.markdown("## ℹ️ What This Means")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### Good News! 🎉")
        st.markdown("""
        **No slow queries were found** matching your search criteria. This indicates:
        
        • **Good Performance**: Your queries are running efficiently
        • **Optimized Workload**: No queries exceeded the 3-minute execution threshold
        • **Well-Tuned System**: Your Snowflake environment is performing well
        
        This is actually a **positive result** - it means your database performance is already optimized for the analyzed period or session.
        """)
        
        # Show search criteria used
        st.markdown("### 🔍 Search Criteria Used")
        if criteria_used:
            criteria_items = []
            if criteria_used.get("session_id"):
                criteria_items.append(f"**Session ID**: {criteria_used['session_id']}")
            if criteria_used.get("query_tag"):
                criteria_items.append(f"**Query Tag**: {criteria_used['query_tag']}")
            if criteria_used.get("start_date"):
                criteria_items.append(f"**Start Date**: {criteria_used['start_date']}")
            if criteria_used.get("query_id"):
                criteria_items.append(f"**Query ID**: {criteria_used['query_id']}")
            if criteria_used.get("execution_time_threshold"):
                criteria_items.append(f"**Time Threshold**: {criteria_used['execution_time_threshold']}")
            
            for item in criteria_items:
                st.markdown(f"• {item}")
        else:
            st.info("Search criteria details not available")
    
    with col2:
        st.markdown("### 💡 Next Steps")
        st.markdown("""
        **Recommendations:**
        
        ✅ **Continue Monitoring**: Keep tracking query performance
        
        🔄 **Try Different Periods**: Check other time ranges if needed
        
        📊 **Expand Analysis**: Analyze other sessions or query tags
        
        🎯 **Proactive Optimization**: Consider analyzing all queries (remove time threshold)
        """)
        
        # Optional: Show how to analyze all queries
        with st.expander("🔧 Analyze All Queries"):
            st.markdown("""
            To analyze **all queries** (not just slow ones), you could:
            
            • Use a broader time range
            • Check different query tags
            • Analyze during peak usage hours
            • Lower the execution time threshold
            """)
    
    # Summary section
    st.divider()
    st.markdown("### 📝 Summary")
    summary_message = report.get("message", "No slow queries found matching the specified criteria.")
    st.success(f"✅ {summary_message}")

def display_results(report):
    """Display analysis results in an organized way."""
    
    # Session Overview
    st.markdown("## 📊 Session Analysis Overview")
    
    # Key metrics in a clean layout - adapt for LangGraph data structure (same as ReAct)
    if "session_analysis" in report or "summary" in report:
        col1, col2, col3, col4 = st.columns(4)
        
        session_info = report.get("session_analysis", {})
        summary_info = report.get("summary", {})
        
        with col1:
            queries_analyzed = session_info.get("total_queries_analyzed", 0)
            create_metric_card("Queries Analyzed", queries_analyzed)
        
        with col2:
            # Map from LangGraph structure (same as ReAct)
            optimized_count = len(report.get("detailed_analyses", []))
            create_metric_card("Optimizations", optimized_count)
        
        with col3:
            # Calculate average from detailed analyses
            detailed_analyses = report.get("detailed_analyses", [])
            if detailed_analyses:
                avg_time = sum(analysis.get("execution_time_seconds", 0) for analysis in detailed_analyses) / len(detailed_analyses)
            else:
                avg_time = 0
            create_metric_card("Avg Execution", f"{avg_time:.2f}s")
        
        with col4:
            # Use estimated improvement from summary
            total_improvement = summary_info.get("estimated_total_improvement", "Unknown")
            create_metric_card("Est. Improvement", total_improvement)
    
    st.divider()
    
    # Query Analyses - map from LangGraph detailed_analyses (same structure as ReAct)
    if "detailed_analyses" in report and report["detailed_analyses"]:
        st.markdown("## 🔍 Query Analysis Results")
        
        # Use tabs for multiple queries - show query IDs as tab names
        if len(report["detailed_analyses"]) > 1:
            # Extract query IDs for tab names, with fallback to generic names
            tab_names = []
            for i, analysis in enumerate(report["detailed_analyses"]):
                query_id = analysis.get("query_id", f"Query {i+1}")
                # Truncate long query IDs for better display
                if len(query_id) > 12:
                    display_id = f"{query_id[:8]}..."
                else:
                    display_id = query_id
                tab_names.append(display_id)
            
            tabs = st.tabs(tab_names)
            for i, (tab, analysis) in enumerate(zip(tabs, report["detailed_analyses"])):
                with tab:
                    display_query_analysis_compact_langgraph(analysis, i + 1, full_report=report)
        else:
            display_query_analysis_compact_langgraph(report["detailed_analyses"][0], 1, full_report=report)
    
    # Infrastructure Recommendations Summary
    display_infrastructure_summary_langgraph(report)


def display_query_analysis_compact_langgraph(analysis, query_num, full_report=None):
    """Display LangGraph query analysis in a more compact, organized way."""
    
    # Quick metrics from LangGraph structure (same as ReAct)
    col1, col2, col3 = st.columns(3)
    with col1:
        exec_time = analysis.get("execution_time_seconds", 0)
        st.metric("Execution Time", f"{exec_time:.2f}s")
    with col2:
        improvement = analysis.get("estimated_performance_gain", "Unknown")
        st.metric("Est. Improvement", improvement)
    with col3:
        # Count optimization recommendations
        opt_recs = analysis.get("optimization_recommendations", [])
        st.metric("Recommendations", len(opt_recs))
    
    # Side-by-side query comparison
    st.markdown("### 📝 Query Comparison")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Original Query**")
        # For LangGraph, get the original query from the analysis data (same as ReAct)
        original_query = analysis.get("original_query_text", "Query text not available")
        if not original_query or original_query == "Query text not available":
            # Fallback: try to get from query_text field
            original_query = analysis.get("query_text", "Query text not available")
        st.text_area(
            "original",
            value=original_query,
            height=250,
            key=f"langgraph_orig_{query_num}",
            label_visibility="collapsed"
        )
    
    with col2:
        st.markdown("**Optimized Query**")
        optimized_query = analysis.get("optimized_query", "No optimized query available")
        if optimized_query:
            st.text_area(
                "optimized",
                value=optimized_query,
                height=250,
                key=f"langgraph_opt_{query_num}",
                label_visibility="collapsed"
            )
        else:
            st.info("No optimized query available")
    
    # CONSOLIDATED PERFORMANCE ANALYSIS SECTION
    st.markdown("---")
    st.markdown("## 🔍 Performance Analysis")
    
    bottlenecks = analysis.get("bottlenecks", [])
    opt_recommendations = analysis.get("optimization_recommendations", [])
    
    if bottlenecks or opt_recommendations:
        # Display Raw Performance Analysis Results
        # Pass the query_id and full_report so the function can access the raw data
        query_id = analysis.get("query_id")
        display_performance_analysis_results(analysis, query_id=query_id, full_report=full_report)
        
        # Separator between sections
        st.markdown("---")
        
        # Third section: AI-Powered Query Optimization Analysis - Full Width
        st.markdown("### 🤖 AI-Powered Query Optimization Analysis")
        st.markdown("*Based on AI analysis of query patterns and optimization opportunities*")
        
        if opt_recommendations:
            create_query_structure_analysis_section(opt_recommendations, original_query)
        else:
            st.info("No structural recommendations available")
                
        # Summary metrics row
        st.markdown("---")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_issues = len(bottlenecks)
            st.metric("Performance Issues", total_issues)
        
        with col2:
            high_severity_count = sum(1 for b in bottlenecks
                                    if (hasattr(b, 'severity') and 'high' in str(b.severity).lower()) or
                                       (isinstance(b, dict) and 'high' in str(b.get('severity', '')).lower()))
            st.metric("High Severity", high_severity_count)
        
        with col3:
            total_recommendations = len(opt_recommendations)
            st.metric("Optimizations", total_recommendations)
        
        with col4:
            # Extract average execution time impact
            percentages = []
            for b in bottlenecks:
                if hasattr(b, 'impact'):
                    impact = b.impact
                elif isinstance(b, dict):
                    impact = b.get('impact', '')
                else:
                    impact = ''
                
                pct_match = re.search(r'(\d+(?:\.\d+)?)\s*%', impact)
                if pct_match:
                    percentages.append(float(pct_match.group(1)))
            
            avg_impact = sum(percentages) / len(percentages) if percentages else 0
            st.metric("Avg Time Impact", f"{avg_impact:.1f}%")
    
    else:
        st.info("No performance issues or recommendations found for this query.")

def display_infrastructure_summary_langgraph(report):
    """Display infrastructure recommendations from LangGraph structure."""
    
    st.markdown("## 🏗️ Infrastructure Recommendations")
    
    # Get aggregated recommendations from LangGraph structure (same as ReAct)
    aggregated_recs = report.get("aggregated_recommendations", {})
    infrastructure_changes = aggregated_recs.get("infrastructure_changes", [])
    
    # Debug: Show what infrastructure changes we have
    if infrastructure_changes:
        st.write(f"🔍 Debug: Found {len(infrastructure_changes)} infrastructure changes")
        # Show the types for debugging
        types_found = []
        for change in infrastructure_changes:
            if hasattr(change, 'type'):  # Pydantic object
                types_found.append(change.type)
            elif isinstance(change, dict):  # Dictionary
                types_found.append(change.get('type', 'Unknown'))
            else:
                types_found.append(str(type(change)))
        st.write(f"🏷️ Types found: {types_found}")
    
    if infrastructure_changes:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🗂️ Schema & Storage")
            
            # Enhanced filtering for schema/storage related infrastructure changes
            schema_keywords = ['table', 'schema', 'cluster', 'partition', 'storage', 'clustering', 'optimization']
            schema_changes = []
            
            for change in infrastructure_changes:
                change_type = ""
                if hasattr(change, 'type'):  # Pydantic object
                    change_type = change.type.lower()
                elif isinstance(change, dict):  # Dictionary
                    change_type = change.get('type', '').lower()
                
                # Check if this change is schema/storage related
                if any(keyword in change_type for keyword in schema_keywords):
                    schema_changes.append(change)
            
            schema_descriptions = []
            for change in schema_changes[:4]:
                # Handle both Pydantic objects and dictionaries
                if hasattr(change, 'type'):  # Pydantic object
                    desc = f"{change.type}: {change.recommendation}"
                elif isinstance(change, dict):  # Dictionary
                    desc = f"{change.get('type', 'Unknown')}: {change.get('recommendation', 'No details')}"
                else:  # String or other
                    desc = str(change)
                schema_descriptions.append(desc)
            
            # If no schema changes found, show debugging info
            if not schema_descriptions:
                st.write("🔍 Debug: No schema changes matched the keywords")
                st.write(f"Keywords searched: {schema_keywords}")
            
            create_info_section("Optimizations", schema_descriptions, "📋")
        
        with col2:
            st.markdown("### ⚡ Performance")
            
            # Enhanced filtering for performance related infrastructure changes
            perf_keywords = ['warehouse', 'cache', 'materialized', 'index', 'sizing', 'compute']
            perf_changes = []
            
            for change in infrastructure_changes:
                change_type = ""
                if hasattr(change, 'type'):  # Pydantic object
                    change_type = change.type.lower()
                elif isinstance(change, dict):  # Dictionary
                    change_type = change.get('type', '').lower()
                
                # Check if this change is performance related
                if any(keyword in change_type for keyword in perf_keywords):
                    perf_changes.append(change)
            
            perf_descriptions = []
            for change in perf_changes[:4]:
                # Handle both Pydantic objects and dictionaries
                if hasattr(change, 'type'):  # Pydantic object
                    desc = f"{change.type}: {change.recommendation}"
                elif isinstance(change, dict):  # Dictionary
                    desc = f"{change.get('type', 'Unknown')}: {change.get('recommendation', 'No details')}"
                else:  # String or other
                    desc = str(change)
                perf_descriptions.append(desc)
            
            # If no performance changes found, show debugging info
            if not perf_descriptions:
                st.write("🔍 Debug: No performance changes matched the keywords")
                st.write(f"Keywords searched: {perf_keywords}")
            
            create_info_section("Improvements", perf_descriptions, "🚀")
        
        # Session-level top recommendations
        if "next_steps" in report:
            with st.expander("📋 Recommended Next Steps"):
                next_steps = report["next_steps"]
                create_info_section("Priority Actions", next_steps[:5], "⭐")
    
    else:
        st.info("No specific infrastructure recommendations found - this could mean:")
        st.write("• The AI didn't generate infrastructure changes")
        st.write("• The query analysis didn't identify infrastructure optimization opportunities")
        st.write("• The aggregated_recommendations structure is missing infrastructure_changes")
        
        # Show general best practices
        with st.expander("💡 General Best Practices"):
            best_practices = [
                "Add clustering keys on frequently filtered columns",
                "Right-size warehouses for your workload",
                "Create materialized views for complex aggregations",
                "Enable result caching for repeated queries",
                "Configure auto-suspend for cost optimization"
            ]
            create_info_section("Snowflake Optimization Tips", best_practices, "✅")


if __name__ == "__main__":
    main()