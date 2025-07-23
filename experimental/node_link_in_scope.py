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



def _stream_links_in_scope(
    self, 
    config: RandomRunConfig
) -> Iterator[int]:
    """Stream links directly using config filters - no intermediate nodes needed."""
    filters = {}
    
    if config.fab_no:
        filters['(n1.fab_no'] = ('=', config.fab_no)
        filters['n2.fab_no)'] = ('=', config.fab_no)
    if config.model_no:
        filters['(n1.model_no'] = ('=', config.model_no) 
        filters['n2.model_no)'] = ('=', config.model_no)
    if config.phase_no:
        filters['(sh1.phase_no'] = ('=', config.phase_no)
        filters['sh2.phase_no)'] = ('=', config.phase_no)
    if config.e2e_group_nos:
        if len(config.e2e_group_nos) == 1:
            filters['(n1.e2e_group_no'] = ('=', config.e2e_group_nos[0])
            filters['n2.e2e_group_no)'] = ('=', config.e2e_group_nos[0])
        else:
            filters['(n1.e2e_group_no'] = ('IN', config.e2e_group_nos)
            filters['n2.e2e_group_no)'] = ('IN', config.e2e_group_nos)
    
    where_clause, params = StringHelper.build_where_clause(filters)
    
    sql = f"""
        SELECT DISTINCT l.link_id
        FROM nw_link l
        JOIN nw_node n1 ON l.start_node_id = n1.node_id
        JOIN nw_node n2 ON l.end_node_id = n2.node_id
        LEFT JOIN org_shape sh1 ON sh1.node_id = n1.node_id
        LEFT JOIN org_shape sh2 ON sh2.node_id = n2.node_id
        {where_clause}
    """
    
    for row in self.db.query_iterator(sql, params):
        yield row[0
