"""
JSON Schema definitions for structured AI responses.

This module contains all the JSON schemas used to ensure consistent
and structured responses from AI/LLM services.
"""

from typing import Dict, Any

# Schema for OperatorStatsAnalysisTool
OPERATOR_STATS_SCHEMA: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "operator_stats_analysis",
        "schema": {
            "type": "object",
            "properties": {
                "top_bottlenecks": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "description": {"type": "string"},
                            "severity": {"type": "string"},
                            "impact": {"type": "string"},
                            "specific_operation": {"type": "string"},
                            "affected_tables": {"type": "array", "items": {"type": "string"}},
                            "performance_metrics": {"type": "object"},
                            "root_cause": {"type": "string"}
                        },
                        "required": ["type", "description", "severity", "impact"]
                    }
                },
                "analysis_summary": {"type": "string"}
            },
            "required": ["top_bottlenecks", "analysis_summary"]
        }
    }
}

# Schema for QueryPerformanceAnalysisTool
QUERY_PERFORMANCE_SCHEMA: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "query_performance_analysis",
        "schema": {
            "type": "object",
            "properties": {
                "performance_analysis": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "description": {"type": "string"},
                            "severity": {"type": "string"},
                            "impact": {"type": "string"},
                            "root_cause": {"type": "string"},
                            "affected_tables": {"type": "array", "items": {"type": "string"}},
                            "resource_impact": {"type": "object"}
                        },
                        "required": ["type", "description", "severity", "impact"]
                    }
                },
                "query_characteristics": {"type": "object"},
                "performance_metrics": {"type": "object"},
                "estimated_performance_gain_potential": {"type": "string"}
            },
            "required": ["performance_analysis", "query_characteristics", "performance_metrics"]
        }
    }
}

# Schema for OptimizedQueryGenerationTool
OPTIMIZATION_SCHEMA: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "optimization_recommendations",
        "schema": {
            "type": "object",
            "properties": {
                "optimization_recommendations": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string"},
                            "description": {"type": "string"},
                            "expected_improvement": {"type": "string"},
                            "ddl_suggestion": {"type": "string"},
                            "priority": {"type": "string", "enum": ["low", "medium", "high", "critical"]}
                        },
                        "required": ["type", "description", "expected_improvement", "priority"]
                    }
                },
                "query_rewrite_needed": {"type": "boolean"},
                "optimized_query": {"type": ["string", "null"]},
                "infrastructure_changes": {"type": "array"},
                "estimated_performance_gain": {"type": "string"}
            },
            "required": ["optimization_recommendations", "query_rewrite_needed", "estimated_performance_gain"]
        }
    }
}

# Schema for QuerySemanticEvaluationTool
SEMANTIC_EVALUATION_SCHEMA: Dict[str, Any] = {
    "type": "json_schema",
    "json_schema": {
        "name": "semantic_evaluation",
        "schema": {
            "type": "object",
            "properties": {
                "semantic_equivalence": {"type": "boolean"},
                "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                "differences_found": {"type": "array", "items": {"type": "string"}},
                "recommendation": {"type": "string", "enum": ["ACCEPT", "REJECT", "RETRY_WITH_FEEDBACK"]},
                "feedback_for_optimization": {"type": ["string", "null"]},
                "detailed_analysis": {"type": "object"}
            },
            "required": ["semantic_equivalence", "confidence_score", "differences_found", "recommendation"]
        }
    }
}