Of course. Creating a streaming version of your original `_fetch_nodes_in_scope` function is an excellent way to handle a very large number of nodes without consuming a lot of memory. This is particularly useful if you need to process the node IDs sequentially for a task other than just fetching links.

Here is the implementation of `_stream_nodes_in_scope` integrated into the previously discussed `OptimizedFetcher` class.

### Code Implementation

The new `_stream_nodes_in_scope` method will use the exact same SQL logic as the original but will call `self.db.query_stream` instead of `self.db.query`. It will then yield the node IDs in chunks.

```python
import gc
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Tuple, Dict

# Assume the existence of these helper and configuration classes:
# class RandomRunConfig: ...
# class StringHelper: ...
# class Database: ...
#     def query(...)
#     def query_stream(...)


class OptimizedFetcher:
    """
    An optimized class to fetch network nodes and links,
    offering both eager and memory-efficient streaming methods.
    """
    def __init__(self, db_connection: 'Database'):
        self.db = db_connection

    def _stream_nodes_in_scope(
        self,
        config: 'RandomRunConfig',
        chunk_size: int = 5000
    ) -> Iterator[List[int]]:
        """
        Streams all node IDs within the specified scope in memory-efficient chunks.

        This method fetches nodes based on the provided filters and yields them
        in lists of a specified size, preventing high memory usage for
        large result sets.

        Args:
            config: Configuration object with filter criteria.
            chunk_size: The number of node IDs to fetch from the database per chunk.

        Yields:
            An iterator that provides lists of node IDs.
        """
        filters: Dict[str, Tuple[str, Any]] = {}
        has_filters = any([config.fab_no, config.model_no, config.phase_no, config.e2e_group_nos])
        if not has_filters:
            # Return an empty iterator if no filters are applied, as fetching
            # all 12.7 million nodes is likely not intended.
            return iter([])

        # --- Filter and Join Logic (identical to the original method) ---
        if config.fab_no:
            filters['n.fab_no'] = ('=', config.fab_no)
        if config.model_no:
            filters['n.model_no'] = ('=', config.model_no)
        if config.e2e_group_nos:
            if len(config.e2e_group_nos) == 1:
                # Use '=' for a single item for potential index optimization
                filters['n.e2e_group_no'] = ('=', config.e2e_group_nos[0])
            else:
                filters['n.e2e_group_no'] = ('IN', config.e2e_group_nos)

        join_clause = ""
        # Conditionally add the JOIN only when it's needed for a filter
        if config.phase_no:
            filters['sh.phase_no'] = ('=', config.phase_no)
            # Assuming table alias 'n' for nw_node and 'sh' for org_shape
            join_clause = "LEFT JOIN org_shape sh ON sh.node_id = n.id"

        where_clause, params = StringHelper.build_where_clause(filters)

        # The table names from your schema are plural (nw_nodes), but the original
        # code used singular aliases. We'll use the plural names here for correctness.
        sql = f"""
            SELECT n.id AS id
            FROM nw_nodes n
            {join_clause}
            {where_clause}
            ORDER BY id
        """

        # Use the streaming query method to yield chunks of results
        for chunk_of_rows in self.db.query_stream(sql, params, chunk_size=chunk_size):
            # Process the chunk of rows to extract just the IDs before yielding
            yield [row[0] for row in chunk_of_rows]

    # --- Other optimized methods from previous answer remain here ---
    # ... (fetch_links_in_scope, stream_links_in_scope, etc.)

```

### How to Use `_stream_nodes_in_scope`

With this new method, you can now process millions of node IDs without loading them all into memory. This is ideal for iterative processing.

```python
# --- Example Usage ---

# Assuming 'fetcher' is an instance of OptimizedFetcher
# and 'run_config' is your RandomRunConfig instance

# This could process 3.7 million nodes without storing them all in a list.
total_nodes_processed = 0
node_processor = MyNodeProcessor() # Some class that processes nodes

# The 'for' loop requests one chunk at a time from the generator
for node_id_chunk in fetcher._stream_nodes_in_scope(run_config, chunk_size=10000):
    # 'node_id_chunk' is a list of 10,000 IDs
    print(f"Processing a chunk of {len(node_id_chunk)} nodes...")
    node_processor.process_batch(node_id_chunk)
    total_nodes_processed += len(node_id_chunk)

print(f"Finished processing a total of {total_nodes_processed} nodes.")
```

### Key Advantages of This Streaming Approach

1.  **Low Memory Footprint**: This is the primary benefit. The application's memory usage is determined by `chunk_size` (e.g., 10,000 integers), not the total result size (e.g., ~4 million integers).
2.  **Responsiveness**: The processing can start as soon as the first chunk of data arrives from the database. You don't have to wait for the entire multi-million-row result set to be downloaded.
3.  **Generator-Based**: By using `yield`, the method becomes a Python generator. This is a clean, idiomatic way to handle streaming data and makes the calling code simple and readable.

While our highly optimized `fetch_links_in_scope` method is still the best solution for your *original problem*, the `_stream_nodes_in_scope` method is a powerful and necessary tool for any other task that requires iterating over a large set of nodes.