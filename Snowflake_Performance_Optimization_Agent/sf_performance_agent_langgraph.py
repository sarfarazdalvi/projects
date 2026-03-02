"""
Snowflake Performance Agent - LangGraph Implementation (Modular Version)
A deterministic agentic AI system using LangGraph for Snowflake SQL performance analysis.

This is the main entry point that imports from the modularized components.
"""

import os
import json
import logging
from typing import Optional, Callable
from datetime import datetime

# Import modular components
from utils.logging_utils import setup_logging
from models.data_models import AIConfig, ProgressUpdate
from workflows.langgraph_workflow import SnowflakePerformanceLangGraphAgent

# Configure logging
logger = setup_logging()

# ============================================================================
# FACTORY FUNCTIONS
# ============================================================================

def create_langgraph_agent(progress_callback: Optional[Callable[[ProgressUpdate], None]] = None) -> SnowflakePerformanceLangGraphAgent:
    """Factory function to create LangGraph agent with Gemini from environment variables."""
    ai_config = AIConfig.from_env()
    
    return SnowflakePerformanceLangGraphAgent(ai_config, progress_callback)

def create_langgraph_agent_from_env(progress_callback: Optional[Callable[[ProgressUpdate], None]] = None) -> SnowflakePerformanceLangGraphAgent:
    """Factory function to create LangGraph agent with Gemini from environment variables."""
    ai_config = AIConfig.from_env()
    
    return SnowflakePerformanceLangGraphAgent(ai_config, progress_callback)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    try:
        # Create LangGraph agent from environment variables
        agent = create_langgraph_agent_from_env()
        
        # Analyze a session using LangGraph deterministic workflow
        session_id = "255102644998495"  # Example session ID
        
        logger.info("=" * 80)
        logger.info("SNOWFLAKE PERFORMANCE AGENT - LANGGRAPH IMPLEMENTATION (MODULAR)")
        logger.info("=" * 80)
        
        # Run the LangGraph analysis
        optimization_report = agent.analyze_session_performance(session_id)
        
        # Display results
        print("\n" + "=" * 80)
        print("OPTIMIZATION REPORT")
        print("=" * 80)
        print(json.dumps(optimization_report, indent=2, default=str))
        
        logger.info("🎉 LangGraph analysis completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error in main execution: {str(e)}")
        print(f"Error: {str(e)}")