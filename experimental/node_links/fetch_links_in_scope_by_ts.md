

```python
def _stream_links_in_scope(
    self, 
    config: RandomRunConfig,
    chunk_size: int = 10000
) -> list[int]:
    """
    Get links by first filtering toolsets, then getting links for each valid toolset.
    
    Args:
        config: Configuration with fab_no, model_no, phase_no, e2e_group_no filters
        chunk_size: Number of rows to fetch per chunk for large result sets
        
    Returns:
        List of all link IDs that match the scope criteria
    """
    # Step 1: Get toolsets that match the node scope filters
    toolset_filters = []
    params = []
    
    if config.fab_no:
        toolset_filters.append("fab_no = ?")
        params.append(config.fab_no)
        
    if config.model_no:
        toolset_filters.append("model_no = ?")
        params.append(config.model_no)
        
    if config.phase_no:
        toolset_filters.append("phase_no = ?")
        params.append(config.phase_no)
        
    if config.e2e_group_nos:
        if len(config.e2e_group_nos) == 1:
            toolset_filters.append("e2e_group_no = ?")
            params.append(config.e2e_group_nos[0])
        else:
            placeholders = ','.join(['?' for _ in config.e2e_group_nos])
            toolset_filters.append(f"e2e_group_no IN ({placeholders})")
            params.extend(config.e2e_group_nos)
    
    if not toolset_filters:
        return []  # No filters, return empty list
    
    where_clause = "WHERE " + " AND ".join(toolset_filters)
    
    # Get valid toolsets
    toolset_sql = f"""
        SELECT fab_no, model_no, phase_no, e2e_group_no
        FROM tb_toolsets
        {where_clause}
        AND is_active = 1
    """
    
    toolsets = self.db.query(toolset_sql, params)
    
    if not toolsets:
        return []
    
    # Step 2: For each toolset, get their links
    all_links = []
    seen_links = set()  # Avoid duplicates
    
    for toolset in toolsets:
        fab_no, model_no, phase_no, e2e_group_no = toolset
        
        # Optimized query for links belonging to this toolset
        links_sql = """
            SELECT l.id
            FROM nw_links l
            WHERE EXISTS (
                SELECT 1 
                FROM nw_nodes n
                WHERE n.id = l.s_node_id
                AND n.fab_no = ? 
                AND n.model_no = ?
                AND n.phase_no = ?
                AND n.e2e_group_no = ?
            )
            OR EXISTS (
                SELECT 1
                FROM nw_nodes n  
                WHERE n.id = l.e_node_id
                AND n.fab_no = ?
                AND n.model_no = ?
                AND n.phase_no = ?
                AND n.e2e_group_no = ?
            )
        """
        
        link_params = [fab_no, model_no, phase_no, e2e_group_no] * 2  # For both EXISTS clauses
        
        # Use query_stream if this toolset might have many links (>50k), otherwise use query
        # Estimate: if we have few toolsets, each might have many links
        if len(toolsets) < 100:  # Few toolsets = potentially many links per toolset
            for chunk in self.db.query_stream(links_sql, link_params, chunk_size):
                for row in chunk:
                    link_id = row[0]
                    if link_id not in seen_links:
                        seen_links.add(link_id)
                        all_links.append(link_id)
        else:  # Many toolsets = fewer links per toolset
            results = self.db.query(links_sql, link_params)
            for row in results:
                link_id = row[0]
                if link_id not in seen_links:
                    seen_links.add(link_id)
                    all_links.append(link_id)
    
    return all_links
```

This approach:
1. **First gets valid toolsets** that match your node scope filters
2. **For each toolset**, gets links where either start or end node belongs to that toolset  
3. **Uses optimized EXISTS queries** with direct column matching (no JOINs on org_shape)
4. **Automatically chooses** `query_stream` vs `query` based on expected result size
5. **Avoids duplicates** using a set while maintaining order in the final list
6. **Returns a complete list** as requested

The query is optimized by using direct column matching in EXISTS clauses rather than complex JOINs, which should be much faster with your indexes.
