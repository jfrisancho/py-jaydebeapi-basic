def fetch_link_ids_in_scope(
    self,
    config: RandomRunConfig,
    fetch_size: int = 10_000
) -> Iterator[int]:
    """
    Stream all DISTINCT link_id values whose start or end node
    matches the RandomRunConfig filters.
    """
    # 1) Build the same filters dict you had
    filters: dict[str, tuple[str, Any]] = {}
    if config.fab_no:
        filters["n.fab_no"] = ("=", config.fab_no)
    if config.model_no:
        filters["n.model_no"] = ("=", config.model_no)
    if config.phase_no:
        filters["sh.phase_no"] = ("=", config.phase_no)
    if config.e2e_group_nos:
        op = "IN" if len(config.e2e_group_nos) > 1 else "="
        filters["n.e2e_group_no"] = (op, config.e2e_group_nos)

    if not filters:
        return  # nothing to do

    where_clause, params = StringHelper.build_where_clause(filters)

    # 2) CTE that picks your nodes, then UNION two selects for start/end links
    sql = f"""
    WITH filtered_nodes AS (
      SELECT n.node_id
      FROM nw_node n
      LEFT JOIN org_shape sh ON sh.node_id = n.node_id
      {where_clause}
    )
    SELECT link_id AS id
    FROM nw_link
    WHERE start_node_id IN (SELECT node_id FROM filtered_nodes)
    UNION
    SELECT link_id AS id
    FROM nw_link
    WHERE end_node_id   IN (SELECT node_id FROM filtered_nodes)
    ORDER BY id
    """

    # 3) Stream it back in batches of `fetch_size`
    for row in self.db.stream_query(sql, params, fetch_size):
        yield row[0]
