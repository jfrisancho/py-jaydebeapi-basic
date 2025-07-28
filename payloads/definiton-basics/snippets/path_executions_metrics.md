Index-friendly way to get your per-run averages at scale:
```
-- 1) Add a covering index so the optimizer can do an index-only scan
ALTER TABLE tb_path_executions
  ADD INDEX idx_run_metrics (
    run_id,
    node_count,
    link_count,
    length_mm
  );

-- 2) Aggregate over run_id
SELECT
  run_id,
  AVG(node_count) AS avg_path_nodes,
  AVG(link_count) AS avg_path_links,
  AVG(length_mm)   AS avg_path_length
FROM tb_path_executions
GROUP BY run_id;
```
Why this is efficient

Covering index on (run_id, node_count, link_count, length_mm) means the database can satisfy the entire query from the index without ever touching the full row (avoiding the CLOB columns)

Grouped by the leading index column (run_id), so the engine can walk the index in order and compute each AVG with minimal sorting or hash-bucket overhead

Scales linearly: even with >2 million rows you’re doing a single index-only scan + grouped aggregation


If you find yourself running this repeatedly on a rapidly growing table, you can further speed things up by:

Periodic summary table (populate via scheduled job) instead of on-the-fly grouping

Partitioning by run date or hash(run_id) if you have very high cardinality in run_id


