from typing import Optional, Iterator, Set
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

class OptimizedScopeHandler:
    """Optimized methods for handling large-scale node and link queries."""
    
    def __init__(self, db: Database):
        self.db = db
        self.logger = logging.getLogger(__name__)
    
    def _fetch_nodes_in_scope_stream(self, config: RandomRunConfig) -> Optional[Iterator[int]]:
        """
        Stream node IDs within the specified scope instead of loading all into memory.
        Memory-efficient version for millions of nodes.
        """
        filters: dict[str, tuple[str, Any]] = {}
        has_filters = any([config.fab_no, config.model_no, config.phase_no, config.e2e_group_nos])
        
        if not has_filters:
            return None
            
        if config.fab_no:
            filters['n.fab_no'] = ('=', config.fab_no)
        if config.model_no:
            filters['n.model_no'] = ('=', config.model_no)
        if config.phase_no:
            filters['sh.phase_no'] = ('=', config.phase_no)
        if config.e2e_group_nos:
            if len(config.e2e_group_nos) == 1:
                filters['n.e2e_group_no'] = ('=', config.e2e_group_nos[0])
            else:
                filters['n.e2e_group_no'] = ('IN', config.e2e_group_nos)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f"""
            SELECT n.node_id AS id
            FROM nw_node n
                LEFT JOIN e2e_group_name_code gn ON gn.code = n.e2e_group_no
                LEFT JOIN org_shape sh ON sh.node_id = n.node_id
            {where_clause}
            ORDER BY id
        """
        
        # Stream nodes instead of loading all at once
        for row in self.db.query_iterator(sql, params):
            yield row[0]
    
    def _stream_links_in_scope(
        self, 
        node_ids: Iterator[int], 
        chunk_size: int = 50000,
        use_temp_table: bool = True
    ) -> Iterator[int]:
        """
        Stream links that connect to nodes in scope, processing in efficient chunks.
        
        Args:
            node_ids: Iterator of node IDs (can be millions)
            chunk_size: Number of nodes to process per chunk
            use_temp_table: Whether to use temporary table for very large datasets
            
        Yields:
            Link IDs that connect to the nodes in scope
        """
        if use_temp_table:
            yield from self._stream_links_with_temp_table(node_ids, chunk_size)
        else:
            yield from self._stream_links_with_chunks(node_ids, chunk_size)
    
    def _stream_links_with_temp_table(
        self, 
        node_ids: Iterator[int], 
        chunk_size: int = 50000
    ) -> Iterator[int]:
        """
        Use temporary table approach for very large node sets (millions of nodes).
        Most efficient for your 3.7M nodes scenario.
        """
        temp_table = f"temp_nodes_{id(self)}"
        
        try:
            # Create temporary table for node IDs
            self.db.update(f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    node_id INTEGER PRIMARY KEY
                )
            """)
            
            # Create index for performance
            self.db.update(f"CREATE INDEX idx_{temp_table}_node_id ON {temp_table}(node_id)")
            
            # Bulk insert node IDs in chunks
            self.logger.info("Inserting nodes into temporary table...")
            total_nodes = 0
            
            def node_chunk_generator():
                chunk = []
                for node_id in node_ids:
                    chunk.append([node_id])
                    if len(chunk) >= chunk_size:
                        yield chunk
                        chunk = []
                if chunk:  # Don't forget the last chunk
                    yield chunk
            
            for chunk in node_chunk_generator():
                # Bulk insert this chunk
                insert_sql = f"INSERT INTO {temp_table} (node_id) VALUES (?)"
                for node_batch in chunk:
                    self.db.update(insert_sql, node_batch)
                
                total_nodes += len(chunk)
                if total_nodes % (chunk_size * 10) == 0:
                    self.logger.info(f"Inserted {total_nodes:,} nodes into temp table")
                    gc.collect()  # Memory management
            
            self.logger.info(f"Completed temp table with {total_nodes:,} nodes")
            
            # Now stream links using the temporary table
            links_sql = f"""
                SELECT DISTINCT l.link_id
                FROM nw_link l
                WHERE EXISTS (SELECT 1 FROM {temp_table} t WHERE t.node_id = l.start_node_id)
                   OR EXISTS (SELECT 1 FROM {temp_table} t WHERE t.node_id = l.end_node_id)
                ORDER BY l.link_id
            """
            
            # Stream the results
            link_count = 0
            for row in self.db.query_iterator(links_sql):
                yield row[0]
                link_count += 1
                if link_count % 100000 == 0:
                    self.logger.info(f"Streamed {link_count:,} links")
            
            self.logger.info(f"Total links found: {link_count:,}")
            
        finally:
            # Clean up temporary table
            try:
                self.db.update(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass  # Ignore cleanup errors
    
    def _stream_links_with_chunks(
        self, 
        node_ids: Iterator[int], 
        chunk_size: int = 10000
    ) -> Iterator[int]:
        """
        Process nodes in chunks using IN clauses. 
        Good for smaller datasets or when temp tables aren't available.
        """
        seen_links: Set[int] = set()  # Track unique links
        chunk_count = 0
        
        def process_node_chunk(node_chunk: list[int]) -> Set[int]:
            """Process a chunk of nodes and return link IDs."""
            if not node_chunk:
                return set()
            
            # Create placeholders for IN clause
            placeholders = ','.join(['?' for _ in node_chunk])
            
            sql = f"""
                SELECT DISTINCT link_id
                FROM nw_link
                WHERE start_node_id IN ({placeholders})
                   OR end_node_id IN ({placeholders})
            """
            
            # Double the parameters for both IN clauses
            params = node_chunk + node_chunk
            
            chunk_links = set()
            try:
                for row in self.db.query_iterator(sql, params):
                    chunk_links.add(row[0])
            except Exception as e:
                self.logger.error(f"Error processing node chunk: {e}")
                raise
            
            return chunk_links
        
        # Process nodes in chunks
        current_chunk = []
        for node_id in node_ids:
            current_chunk.append(node_id)
            
            if len(current_chunk) >= chunk_size:
                chunk_count += 1
                chunk_links = process_node_chunk(current_chunk)
                
                # Yield new links (avoid duplicates)
                for link_id in chunk_links:
                    if link_id not in seen_links:
                        seen_links.add(link_id)
                        yield link_id
                
                # Log progress
                if chunk_count % 100 == 0:
                    self.logger.info(f"Processed {chunk_count} chunks, found {len(seen_links):,} unique links")
                
                current_chunk = []
                gc.collect()  # Memory management
        
        # Process final chunk
        if current_chunk:
            chunk_links = process_node_chunk(current_chunk)
            for link_id in chunk_links:
                if link_id not in seen_links:
                    seen_links.add(link_id)
                    yield link_id
        
        self.logger.info(f"Total unique links found: {len(seen_links):,}")
    
    def _parallel_stream_links_in_scope(
        self, 
        node_ids: Iterator[int], 
        chunk_size: int = 20000,
        max_workers: int = 4
    ) -> Iterator[int]:
        """
        Parallel processing version for maximum performance.
        Uses multiple threads to process node chunks simultaneously.
        """
        from queue import Queue, Empty
        import threading
        
        # Thread-safe queue for results
        result_queue = Queue()
        seen_links: Set[int] = set()
        processing_complete = threading.Event()
        
        def node_chunk_processor(node_chunk: list[int], worker_id: int):
            """Process a chunk of nodes in a separate thread."""
            try:
                if not node_chunk:
                    return
                
                placeholders = ','.join(['?' for _ in node_chunk])
                sql = f"""
                    SELECT DISTINCT link_id
                    FROM nw_link
                    WHERE start_node_id IN ({placeholders})
                       OR end_node_id IN ({placeholders})
                """
                
                params = node_chunk + node_chunk
                
                # Create a separate database connection for this thread
                with Database(self.db.config) as thread_db:
                    chunk_links = []
                    for row in thread_db.query_iterator(sql, params):
                        chunk_links.append(row[0])
                    
                    # Put results in queue
                    result_queue.put(('links', chunk_links, worker_id))
                    
            except Exception as e:
                self.logger.error(f"Worker {worker_id} error: {e}")
                result_queue.put(('error', str(e), worker_id))
        
        def result_processor():
            """Process results from worker threads."""
            while not processing_complete.is_set() or not result_queue.empty():
                try:
                    result_type, data, worker_id = result_queue.get(timeout=1.0)
                    
                    if result_type == 'links':
                        for link_id in data:
                            if link_id not in seen_links:
                                seen_links.add(link_id)
                                yield link_id
                    elif result_type == 'error':
                        self.logger.error(f"Worker {worker_id} reported error: {data}")
                        
                except Empty:
                    continue
                except Exception as e:
                    self.logger.error(f"Result processor error: {e}")
        
        # Start result processor in separate thread
        result_thread = threading.Thread(target=result_processor)
        result_thread.start()
        
        # Process nodes in parallel chunks
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            current_chunk = []
            chunk_id = 0
            
            for node_id in node_ids:
                current_chunk.append(node_id)
                
                if len(current_chunk) >= chunk_size:
                    # Submit chunk for processing
                    future = executor.submit(node_chunk_processor, current_chunk.copy(), chunk_id)
                    futures.append(future)
                    
                    current_chunk = []
                    chunk_id += 1
                    
                    # Log progress
                    if chunk_id % 50 == 0:
                        self.logger.info(f"Submitted {chunk_id} chunks for processing")
            
            # Submit final chunk
            if current_chunk:
                future = executor.submit(node_chunk_processor, current_chunk, chunk_id)
                futures.append(future)
            
            # Wait for all chunks to complete
            for future in as_completed(futures):
                try:
                    future.result()  # This will raise any exceptions
                except Exception as e:
                    self.logger.error(f"Chunk processing failed: {e}")
        
        # Signal completion
        processing_complete.set()
        result_thread.join()
        
        self.logger.info(f"Parallel processing complete. Found {len(seen_links):,} unique links")
    
    def get_scope_statistics(self, config: RandomRunConfig) -> dict[str, Any]:
        """
        Get statistics about the scope without loading all data into memory.
        Useful for planning the optimal processing strategy.
        """
        stats = {
            'estimated_nodes': 0,
            'estimated_links': 0,
            'processing_strategy': 'unknown',
            'recommended_chunk_size': 10000,
            'use_temp_table': False,
            'use_parallel': False
        }
        
        # Estimate node count
        filters: dict[str, tuple[str, Any]] = {}
        if config.fab_no:
            filters['n.fab_no'] = ('=', config.fab_no)
        if config.model_no:
            filters['n.model_no'] = ('=', config.model_no)
        if config.phase_no:
            filters['sh.phase_no'] = ('=', config.phase_no)
        if config.e2e_group_nos:
            if len(config.e2e_group_nos) == 1:
                filters['n.e2e_group_no'] = ('=', config.e2e_group_nos[0])
            else:
                filters['n.e2e_group_no'] = ('IN', config.e2e_group_nos)
        
        if filters:
            where_clause, params = StringHelper.build_where_clause(filters)
            
            count_sql = f"""
                SELECT COUNT(*) as node_count
                FROM nw_node n
                    LEFT JOIN e2e_group_name_code gn ON gn.code = n.e2e_group_no
                    LEFT JOIN org_shape sh ON sh.node_id = n.node_id
                {where_clause}
            """
            
            try:
                result = self.db.query(count_sql, params)
                if result:
                    stats['estimated_nodes'] = result[0][0]
                    
                    # Determine optimal strategy based on node count
                    node_count = stats['estimated_nodes']
                    
                    if node_count > 1_000_000:  # > 1M nodes
                        stats['processing_strategy'] = 'temp_table'
                        stats['recommended_chunk_size'] = 50000
                        stats['use_temp_table'] = True
                        stats['use_parallel'] = True
                    elif node_count > 100_000:  # 100K - 1M nodes
                        stats['processing_strategy'] = 'chunked'
                        stats['recommended_chunk_size'] = 20000
                        stats['use_parallel'] = True
                    else:  # < 100K nodes
                        stats['processing_strategy'] = 'simple'
                        stats['recommended_chunk_size'] = 10000
                    
                    # Estimate link count (rough approximation)
                    # Assuming average connectivity, adjust based on your network topology
                    avg_links_per_node = 2.5  # Adjust based on your network characteristics
                    stats['estimated_links'] = int(node_count * avg_links_per_node)
                    
            except Exception as e:
                self.logger.error(f"Error getting scope statistics: {e}")
        
        return stats

# Usage example with your existing code pattern
class NetworkScopeProcessor:
    """Main class integrating the optimized scope methods."""
    
    def __init__(self, db: Database):
        self.db = db
        self.scope_handler = OptimizedScopeHandler(db)
        self.logger = logging.getLogger(__name__)
    
    def process_scope_efficiently(self, config: RandomRunConfig) -> Iterator[tuple[int, int]]:
        """
        Process both nodes and links in scope with optimal memory usage.
        Returns iterator of (node_id, link_id) pairs.
        """
        # Get scope statistics to choose optimal strategy
        stats = self.scope_handler.get_scope_statistics(config)
        self.logger.info(f"Scope statistics: {stats}")
        
        # Stream nodes
        node_stream = self.scope_handler._fetch_nodes_in_scope_stream(config)
        if not node_stream:
            self.logger.info("No nodes in scope")
            return
        
        # Choose optimal link processing strategy
        if stats['use_temp_table']:
            self.logger.info("Using temporary table strategy for links")
            link_stream = self.scope_handler._stream_links_in_scope(
                node_stream, 
                chunk_size=stats['recommended_chunk_size'],
                use_temp_table=True
            )
        elif stats['use_parallel']:
            self.logger.info("Using parallel processing strategy for links")
            # Convert iterator to list for parallel processing (if memory allows)
            # For 3.7M nodes, consider using temp table instead
            link_stream = self.scope_handler._stream_links_in_scope(
                node_stream,
                chunk_size=stats['recommended_chunk_size'],
                use_temp_table=False
            )
        else:
            self.logger.info("Using simple chunked strategy for links")
            link_stream = self.scope_handler._stream_links_in_scope(
                node_stream,
                chunk_size=stats['recommended_chunk_size'],
                use_temp_table=False
            )
        
        # Stream results
        link_count = 0
        for link_id in link_stream:
            yield link_id
            link_count += 1
            
            if link_count % 100000 == 0:
                self.logger.info(f"Processed {link_count:,} links")
        
        self.logger.info(f"Scope processing complete. Total links: {link_count:,}")

# Example usage replacing your original methods
"""
# Replace your original methods with:

# Instead of:
# node_ids = self._fetch_nodes_in_scope(config)  # Loads 3.7M into memory
# link_ids = self._fetch_links_in_scope(node_ids)  # Memory explosion

# Use:
processor = NetworkScopeProcessor(self.db)
for link_id in processor.process_scope_efficiently(config):
    # Process each link ID as it's streamed
    process_link(link_id)
    
# Or if you need the original pattern:
def _stream_links_in_scope_optimized(self, config: RandomRunConfig) -> Iterator[int]:
    scope_handler = OptimizedScopeHandler(self.db)
    node_stream = scope_handler._fetch_nodes_in_scope_stream(config)
    if node_stream:
        yield from scope_handler._stream_links_in_scope(
            node_stream, 
            chunk_size=50000,  # Optimal for your 3.7M nodes
            use_temp_table=True  # Best for large datasets
        )
"""