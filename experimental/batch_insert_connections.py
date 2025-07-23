forbidden = set(self.forbidden_node_ids)
existing = set(self.node_ids)

additional_node_ids: set[int] = {
    node_id
    for row in link_rows
    if row
    for node_id in (row[2], row[3])
    if node_id not in forbidden and node_id not in existing
}



def batch_insert_connections(db: Database, connection_data: list[tuple]) -> int:
    """
    Insert equipment connection data using optimized batch execution for large datasets.
    
    Args:
        db: Database instance
        connection_data: List of validated connection tuples
        
    Returns:
        Total number of rows inserted
    """
    if not connection_data:
        print('No equipment connection data to insert')
        return 0
    
    insert_sql = '''
        INSERT INTO tb_equipment_connections 
        (from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type)
        VALUES (?, ?, ?, ?, ?, ?)
    '''
    
    # Convert tuples to list of lists for execute_batch
    params_list = []
    for connection in connection_data:
        from_equipment_id, to_equipment_id, from_poc_id, to_poc_id, is_valid, connection_type = connection
        params_list.append([from_equipment_id, to_equipment_id, from_poc_id, 
                           to_poc_id, is_valid, connection_type])
    
    print(f'Starting batch insert for {len(connection_data)} equipment connections...')
    
    try:
        total_inserted = db.execute_batch(insert_sql, params_list)
        print(f'✓ Successfully inserted {total_inserted} equipment connections')
        return total_inserted
        
    except Exception as e:
        print(f'✗ Failed to insert equipment connections: {e}')
        raise


def stream_query(
        self,
        sql: str,
        params: Optional[list[Any]] = None,
        fetch_size: int = 5_000,
    ) -> Iterator[tuple]:
        """
        Execute a SELECT but fetch in batches of `fetch_size`, yielding row by row.
        """
        cur = self._conn.cursor()
        try:
            cur.execute(sql, params or [])
            while True:
                batch = cur.fetchmany(fetch_size)
                if not batch:
                    break
                for row in batch:
                    yield row
        finally:
            cur.close()

for node_id, s_node, e_node in db.stream_query(
    "SELECT id, s_node_id, e_node_id FROM nw_links WHERE …",
    fetch_size=10_000
):
    # process one link at a time, tiny memory footprint
