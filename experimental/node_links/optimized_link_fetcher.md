Based on your code and performance issue, you've correctly identified the major bottleneck: fetching nearly 4 million node IDs from the database to the Python client, only to send them right back in a massive `IN` clause. This is extremely inefficient and puts unnecessary strain on both the database and the application's memory.

Your proposal to use "toolsets" or, more generally, to apply the initial filters directly to the link-finding query is the correct approach. We can construct a single, optimized SQL query that finds the links without the expensive intermediate step.

Here is a refactored and optimized implementation using Python 3.11.

### High-Level Strategy

1.  **Eliminate Intermediate Node Fetching**: We will bypass the `_fetch_nodes_in_scope` function entirely. Its sole purpose was to gather IDs for the next step, which we are now merging into a single query.
2.  **Directly Query Links**: We will create a new method that directly queries the `nw_link` table.
3.  **Apply Filters on Joined Nodes**: The query will join `nw_link` with `nw_node` and apply the filters from `RandomRunConfig` directly to the node properties.
4.  **Use `UNION` for Performance**: To find links where *either* the start node or the end node matches the criteria, a `UNION` is generally more performant than using `OR` on joined tables. It allows the database to use indexes effectively for two separate, simple queries and then combine the distinct results.
5.  **Provide Both Eager and Streaming Methods**: I will provide an optimized method (`_fetch_links_in_scope`) that fetches all results at once and a streaming method (`_stream_links_in_scope`) for scenarios where the resulting link list could also be too large for memory.

---

### Optimized Code Implementation

Here is the new, more efficient class structure. Note that `_fetch_nodes_in_scope` is no longer needed for this process.

```python
import gc
from contextlib import contextmanager
from typing import Any, Iterator, List, Optional, Tuple, Dict

# Assume the existence of these classes from your original code
# class RandomRunConfig: ...
# class StringHelper: ...
# class Database: ...


class OptimizedLinkFetcher:
    """
    An optimized class to fetch network links based on node properties,
    avoiding inefficient intermediate steps.
    """
    def __init__(self, db_connection):
        self.db = db_connection

    def _build_link_query_components(
        self,
        config: 'RandomRunConfig'
    ) -> Tuple[str, str, List[Any]]:
        """
        Builds the dynamic parts of the SQL query (JOINs, WHERE clause, and params).

        This helper prevents code duplication for the UNION query.
        """
        filters: Dict[str, Tuple[str, Any]] = {}
        joins = []

        if config.fab_no:
            filters['n.fab_no'] = ('=', config.fab_no)
        if config.model_no:
            filters['n.model_no'] = ('=', config.model_no)
        if config.e2e_group_nos:
            if len(config.e2e_group_nos) == 1:
                filters['n.e2e_group_no'] = ('=', config.e2e_group_nos[0])
            else:
                filters['n.e2e_group_no'] = ('IN', config.e2e_group_nos)

        # The join to 'org_shape' is conditional, only add it if needed.
        if config.phase_no:
            filters['sh.phase_no'] = ('=', config.phase_no)
            joins.append("LEFT JOIN org_shape sh ON sh.node_id = n.id")

        join_clause = " ".join(joins)
        where_clause, params = StringHelper.build_where_clause(filters)

        return join_clause, where_clause, params

    def fetch_links_in_scope(
        self,
        config: 'RandomRunConfig'
    ) -> List[int]:
        """
        Fetches all link IDs within the specified scope using an optimized
        UNION query.

        This method directly queries links whose start or end nodes match the
        filter criteria, completely avoiding the need to fetch all node IDs first.
        """
        has_filters = any([config.fab_no, config.model_no, config.phase_no, config.e2e_group_nos])
        if not has_filters:
            # If no filters, it implies all links. This could be dangerous,
            # so we return empty or handle as per business logic.
            # Returning empty is safer.
            return []

        join_sql, where_sql, params = self._build_link_query_components(config)

        # The UNION operator automatically returns distinct link IDs.
        # This is much more efficient than a single query with 'OR' on different nodes.
        sql = f"""
            SELECT l.id FROM nw_link l
            INNER JOIN nw_node n ON l.s_node_id = n.id
            {join_sql}
            {where_sql}

            UNION

            SELECT l.id FROM nw_link l
            INNER JOIN nw_node n ON l.e_node_id = n.id
            {join_sql}
            {where_sql}

            ORDER BY id
        """

        # The parameters must be duplicated for both sides of the UNION
        full_params = params + params
        results = self.db.query(sql, full_params)
        return [row[0] for row in results]

    def stream_links_in_scope(
        self,
        config: 'RandomRunConfig',
        chunk_size: int = 5000
    ) -> Iterator[List[int]]:
        """
        Streams link IDs in chunks for the specified scope.

        This is a memory-efficient alternative to `fetch_links_in_scope`,
        ideal for when the number of resulting links is very large.
        """
        has_filters = any([config.fab_no, config.model_no, config.phase_no, config.e2e_group_nos])
        if not has_filters:
            return iter([]) # Return an empty iterator

        join_sql, where_sql, params = self._build_link_query_components(config)

        sql = f"""
            SELECT l.id FROM nw_link l
            INNER JOIN nw_node n ON l.s_node_id = n.id
            {join_sql}
            {where_sql}

            UNION

            SELECT l.id FROM nw_link l
            INNER JOIN nw_node n ON l.e_node_id = n.id
            {join_sql}
            {where_sql}

            ORDER BY id
        """
        full_params = params + params
        
        # Use the query_stream method to yield chunks of link IDs
        for chunk in self.db.query_stream(sql, full_params, chunk_size=chunk_size):
            yield [row[0] for row in chunk]

```

### Explanation of Improvements and Optimizations

1.  **Single Database Round-Trip**: The new implementation combines the node filtering and link fetching into a single, powerful SQL query. This eliminates the need to transfer millions of node IDs from the database to the application and back again, which is the biggest performance gain.
2.  **Efficient `UNION` Query**:
    *   The `UNION` approach allows the database to execute two simple, fast queries and then merge the results.
    *   Each subquery can efficiently use indexes on `nw_link` (`s_node_id`, `e_node_id`) and `nw_node` (`fab_no`, `model_no`, etc.).
    *   This is typically much faster than a query with a complex `OR` condition spanning different tables (e.g., `... WHERE (start_node_matches) OR (end_node_matches)`), which often hinders index usage.
    *   `UNION` (as opposed to `UNION ALL`) implicitly handles `DISTINCT`, giving us the unique link IDs we need.
3.  **Reduced Application Memory**: The Python application no longer needs to hold a list of ~4 million node IDs in memory. The memory footprint is significantly reduced, especially in the streaming version.
4.  **Conditional Joins**: The `LEFT JOIN` to the `org_shape` table is only included in the query if a `phase_no` filter is actually provided. This keeps the query as lean as possible when that filter is not used.
5.  **Streaming for Large Result Sets**: The `stream_links_in_scope` method is now available for cases where even the *final list of links* is too large to process in memory at once. It uses the `query_stream` generator to yield chunks of results, providing excellent memory efficiency for downstream processing.
6.  **Code Simplification**: By removing the `_fetch_nodes_in_scope` and `_fetch_links_in_scope` combination, the overall logic is simpler, more direct, and easier to maintain. The intent of the code—to get links based on node properties—is now much clearer.
7.  **Database-Side Optimization**: This approach shifts the workload from the client application to the database, which is what databases are designed and optimized for: efficiently joining, filtering, and retrieving large datasets.