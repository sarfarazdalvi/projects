"""
Constants used throughout the Snowflake Performance Agent.
"""

OPTIMIZATION_RULES_TEXT = """
When optimizing a query or table, follow this strict priority order:
1) SQL/query optimization → 2) Schema/model → 3) Table-level services.
Only recommend costlier levers (clustering, SOS, MVs) when justified by symptoms

Optimization Rules (Use in Order)
1. SQL & Query Design Best Practices (highest priority)
*CRITICAL REQUIREMENT*: if the JOIN or WHERE condition contains COALESCE(<column>, default_value), do to remove COALESCE unless a workaround is provided.
*IMPORTANT SUGGESTION*: If the data from large table can be aggregated before joining to other tables to reduce records for join, do so. 
Select only required columns (avoid SELECT *).
Use sargable predicates (avoid wrapping columns in functions).
Push filters down; simplify joins and predicates.
Ensure join keys have matching data types.
Avoid repeated expressions; precompute if reused.
Aggregate only necessary columns.
Encourage repeatable queries to leverage result cache.


2. Schema & Data Model Best Practices
Use correct/smaller data types.
Normalize wide tables when beneficial.
Prefer large batch loads to avoid micro-partition fragmentation.
Order data on load by commonly filtered columns (e.g., date).

3. Table-Level Optimization Levers (use only when needed)
Micro-partition pruning (default; improves with ordered loads).
Clustering keys for range scans or predictable filter patterns.
Automatic Clustering when tables are large + frequently updated.
Search Optimization Service (SOS) for point lookups / selective filters.
Materialized Views (MV) for repeated heavy joins/aggregations.
MV-specific clustering when MV serves different patterns than base table.
External table partitioning for data stored in S3/GCS/Azure.
Reclustering for fragmented tables.
Transient/temporary tables for reducing retention overhead."""