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