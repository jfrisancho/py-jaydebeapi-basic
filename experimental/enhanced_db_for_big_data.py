import jaydebeapi  
from contextlib import contextmanager  
from typing import List, Optional, Dict, Any, Union, Iterator, Tuple, Callable
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH  
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue, Empty
import gc

@dataclass
class QueryConfig:
    """Configuration for large dataset queries"""
    fetch_size: int = 10000
    max_memory_rows: int = 100000
    enable_streaming: bool = True
    parallel_chunks: int = 4
    commit_interval: int = 1000

class Database:  
    """  
    Enhanced database class optimized for spatial databases and big data operations.
    Supports millions of rows with streaming, chunked processing, and memory management.
    """  
  
    def __init__(self, query_config: Optional[QueryConfig] = None):  
        """  
        Initialize database connection with optional performance configuration.
        
        Args:
            query_config: Configuration for handling large datasets
        """  
        self.config = query_config or QueryConfig()
        self._local = threading.local()
        
        try:  
            self._conn = jaydebeapi.connect(  
                DRIVER_CLASS,  
                JDBC_URL,  
                [DB_USER, DB_PASSWORD],  
                DRIVER_PATH  
            )
            # Set fetch size for better performance with large result sets
            self._conn.jconn.setAutoCommit(False)
        except jaydebeapi.DatabaseError as e:  
            raise RuntimeError(f"Failed to connect via JDBC: {e}")  
  
    @contextmanager  
    def cursor(self, fetch_size: Optional[int] = None):  
        """  
        Provide a cursor with optimized fetch size for large datasets.
        """  
        cur = None  
        try:  
            cur = self._conn.cursor()  
            # Set fetch size to optimize memory usage and network roundtrips
            if hasattr(cur, 'setFetchSize'):
                cur.setFetchSize(fetch_size or self.config.fetch_size)
            yield cur  
        finally:  
            if cur:  
                cur.close()  

    def query_stream(self, sql: str, params: Optional[List] = None, 
                    chunk_size: Optional[int] = None) -> Iterator[List]:
        """
        Execute a SELECT statement and yield results in chunks to handle millions of rows.
        Memory-efficient streaming of large result sets.
        
        Args:
            sql: SELECT SQL statement
            params: Query parameters
            chunk_size: Number of rows to fetch per chunk
            
        Yields:
            List of rows (chunk_size rows at a time)
            
        Example:
            for chunk in db.query_stream("SELECT * FROM large_table", chunk_size=5000):
                process_chunk(chunk)
        """
        chunk_size = chunk_size or self.config.fetch_size
        
        with self.cursor(fetch_size=chunk_size) as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield rows
                
                # Explicit garbage collection for very large datasets
                if len(rows) == chunk_size:
                    gc.collect()

    def query_iterator(self, sql: str, params: Optional[List] = None) -> Iterator[Tuple]:
        """
        Execute a SELECT statement and yield individual rows.
        Most memory-efficient for processing row by row.
        
        Args:
            sql: SELECT SQL statement  
            params: Query parameters
            
        Yields:
            Individual database rows
            
        Example:
            for row in db.query_iterator("SELECT * FROM huge_spatial_table"):
                process_single_row(row)
        """
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            
            while True:
                row = cur.fetchone()
                if row is None:
                    break
                yield row

    def query_with_callback(self, sql: str, callback: Callable[[List], None], 
                           params: Optional[List] = None, 
                           chunk_size: Optional[int] = None) -> int:
        """
        Execute query and process results with a callback function.
        Useful for ETL operations or data transformations on large datasets.
        
        Args:
            sql: SELECT SQL statement
            callback: Function to process each chunk of rows
            params: Query parameters  
            chunk_size: Rows per chunk
            
        Returns:
            Total number of rows processed
            
        Example:
            def process_spatial_chunk(rows):
                # Process spatial data
                for row in rows:
                    calculate_distances(row)
                    
            total = db.query_with_callback(
                "SELECT node_id, ST_X(geom), ST_Y(geom) FROM nodes", 
                process_spatial_chunk
            )
        """
        total_rows = 0
        chunk_size = chunk_size or self.config.fetch_size
        
        for chunk in self.query_stream(sql, params, chunk_size):
            callback(chunk)
            total_rows += len(chunk)
            
        return total_rows

    def query_parallel_chunks(self, sql: str, partition_column: str, 
                             callback: Callable[[List], Any],
                             params: Optional[List] = None,
                             num_partitions: Optional[int] = None) -> List[Any]:
        """
        Execute query in parallel chunks based on a partition column.
        Excellent for spatial queries with geographic partitioning.
        
        Args:
            sql: Base SQL statement (will be modified with WHERE conditions)
            partition_column: Column to partition on (e.g., region_id, grid_cell)
            callback: Function to process each partition
            params: Base query parameters
            num_partitions: Number of parallel partitions
            
        Returns:
            List of results from each partition
            
        Example:
            def process_region(rows):
                return analyze_network_topology(rows)
                
            results = db.query_parallel_chunks(
                "SELECT * FROM network_links",
                "region_id", 
                process_region,
                num_partitions=8
            )
        """
        num_partitions = num_partitions or self.config.parallel_chunks
        
        # Get partition values
        partition_sql = f"SELECT DISTINCT {partition_column} FROM ({sql}) subq ORDER BY {partition_column}"
        partitions = [row[0] for row in self.query(partition_sql, params)]
        
        # Group partitions into chunks
        partition_chunks = [partitions[i::num_partitions] for i in range(num_partitions)]
        
        def process_partition_chunk(partition_values):
            chunk_results = []
            for partition_value in partition_values:
                partition_where = f" AND {partition_column} = ?"
                partition_sql = sql + partition_where
                partition_params = (params or []) + [partition_value]
                
                rows = self.query(partition_sql, partition_params)
                if rows:
                    result = callback(rows)
                    chunk_results.append(result)
            return chunk_results
        
        # Execute in parallel
        results = []
        with ThreadPoolExecutor(max_workers=num_partitions) as executor:
            futures = [executor.submit(process_partition_chunk, chunk) for chunk in partition_chunks]
            for future in as_completed(futures):
                results.extend(future.result())
                
        return results

    def bulk_insert_stream(self, table: str, columns: List[str], 
                          data_stream: Iterator[List], 
                          batch_size: Optional[int] = None) -> int:
        """
        Efficiently insert millions of rows using streaming and batching.
        
        Args:
            table: Target table name
            columns: List of column names
            data_stream: Iterator yielding batches of rows
            batch_size: Rows per batch commit
            
        Returns:
            Total number of rows inserted
            
        Example:
            def generate_equipment_data():
                for i in range(1000000):
                    yield [i, f'Equipment_{i}', random_location()]
                    
            db.bulk_insert_stream(
                'equipment', 
                ['id', 'name', 'location'],
                generate_equipment_data(),
                batch_size=5000
            )
        """
        batch_size = batch_size or self.config.commit_interval
        placeholders = ','.join(['?' for _ in columns])
        insert_sql = f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders})"
        
        total_inserted = 0
        batch_count = 0
        
        with self.cursor() as cur:
            for row in data_stream:
                cur.execute(insert_sql, row)
                total_inserted += 1
                batch_count += 1
                
                if batch_count >= batch_size:
                    self._conn.commit()
                    batch_count = 0
                    
                    # Memory management for large operations
                    if total_inserted % (batch_size * 10) == 0:
                        gc.collect()
                        logging.info(f"Inserted {total_inserted} rows")
            
            # Commit any remaining rows
            if batch_count > 0:
                self._conn.commit()
                
        return total_inserted

    def spatial_query_with_bounds(self, sql: str, bounds_column: str,
                                 min_x: float, min_y: float, 
                                 max_x: float, max_y: float,
                                 params: Optional[List] = None,
                                 chunk_size: Optional[int] = None) -> Iterator[List]:
        """
        Execute spatial query with bounding box optimization.
        Useful for processing spatial data in geographic regions.
        
        Args:
            sql: Base spatial SQL
            bounds_column: Spatial column name (e.g., 'geom', 'location')
            min_x, min_y, max_x, max_y: Bounding box coordinates
            params: Additional parameters
            chunk_size: Rows per chunk
            
        Yields:
            Chunks of rows within the bounding box
            
        Example:
            # Process all network nodes in a specific region
            for chunk in db.spatial_query_with_bounds(
                "SELECT node_id, ST_AsText(geom) FROM network_nodes",
                "geom", -74.1, 40.7, -73.9, 40.8  # NYC bounds
            ):
                process_nyc_nodes(chunk)
        """
        # Add spatial bounds to query - syntax may vary by spatial DB
        spatial_where = f" AND {bounds_column} && ST_MakeEnvelope(?, ?, ?, ?, 4326)"
        bounded_sql = sql + spatial_where
        bounded_params = (params or []) + [min_x, min_y, max_x, max_y]
        
        yield from self.query_stream(bounded_sql, bounded_params, chunk_size)

    def get_table_stats(self, table: str) -> Dict[str, Any]:
        """
        Get statistics about a table to help optimize queries.
        
        Args:
            table: Table name
            
        Returns:
            Dictionary with row count and other statistics
        """
        stats = {}
        
        # Row count
        count_result = self.query(f"SELECT COUNT(*) FROM {table}")
        stats['row_count'] = count_result[0][0] if count_result else 0
        
        # Try to get additional stats (may vary by database)
        try:
            # This is database-specific - adjust for your spatial DB
            size_result = self.query(f"SELECT pg_size_pretty(pg_total_relation_size('{table}'))")
            stats['table_size'] = size_result[0][0] if size_result else 'Unknown'
        except:
            stats['table_size'] = 'Unknown'
            
        return stats

    def create_spatial_index_hint(self, table: str, geom_column: str = 'geom') -> str:
        """
        Generate index creation hint for spatial columns.
        
        Args:
            table: Table name
            geom_column: Geometry column name
            
        Returns:
            SQL statement to create spatial index
        """
        # Generic spatial index - adjust for your specific spatial database
        return f"CREATE INDEX IF NOT EXISTS idx_{table}_{geom_column}_spatial ON {table} USING GIST ({geom_column})"

    def query(self, sql: str, params: Optional[List] = None) -> List:  
        """  
        Execute a SELECT statement and return all rows.
        For large datasets, consider using query_stream() instead.
        """  
        with self.cursor() as cur:  
            if params:  
                cur.execute(sql, params)  
            else:  
                cur.execute(sql)  
            return cur.fetchall()  
  
    def update(self, sql: str, params: Optional[List] = None) -> int:  
        """  
        Execute an INSERT / UPDATE / DELETE. Return number of affected rows.  
        """  
        with self.cursor() as cur:  
            if params:  
                cur.execute(sql, params)  
            else:  
                cur.execute(sql)  
            self._conn.commit()  
            return cur.rowcount  

    def callproc(self, proc_name: str, params: Optional[List] = None) -> Optional[List]:  
        """  
        Call a stored procedure using CALL statement.  
        """  
        with self.cursor() as cur:  
            if params:  
                placeholders = ','.join(['?' for _ in params])  
                call_sql = f"CALL {proc_name}({placeholders})"  
                cur.execute(call_sql, params)  
            else:  
                call_sql = f"CALL {proc_name}()"  
                cur.execute(call_sql)  
              
            self._conn.commit()  
              
            try:  
                return cur.fetchall()  
            except:  
                return None  
  
    def callproc_with_output(self, proc_name: str, in_params: Optional[List] = None,   
                           out_param_count: int = 0) -> Dict[str, Any]:  
        """  
        Call a stored procedure that has output parameters.  
        """  
        with self.cursor() as cur:  
            total_params = (len(in_params) if in_params else 0) + out_param_count  
              
            if total_params > 0:  
                placeholders = ','.join(['?' for _ in range(total_params)])  
                call_sql = f"CALL {proc_name}({placeholders})"  
                all_params = (in_params or []) + [None] * out_param_count  
                cur.execute(call_sql, all_params)  
            else:  
                call_sql = f"CALL {proc_name}()"  
                cur.execute(call_sql)  
              
            self._conn.commit()  
              
            result = {  
                'result_set': None,  
                'output_params': []  
            }  
              
            try:  
                result['result_set'] = cur.fetchall()  
            except:  
                pass  
              
            return result  
  
    def call_function(self, func_name: str, params: Optional[List] = None) -> Any:  
        """  
        Call a database function (returns a single value).  
        """  
        with self.cursor() as cur:  
            if params:  
                placeholders = ','.join(['?' for _ in params])  
                call_sql = f"SELECT {func_name}({placeholders})"  
                cur.execute(call_sql, params)  
            else:  
                call_sql = f"SELECT {func_name}()"  
                cur.execute(call_sql)  
              
            result = cur.fetchone()  
            return result[0] if result else None  
  
    def execute_batch(self, sql: str, params_list: List[List]) -> int:  
        """  
        Execute the same SQL statement multiple times with different parameters.  
        """  
        with self.cursor() as cur:  
            total_affected = 0  
            for params in params_list:  
                cur.execute(sql, params)  
                total_affected += cur.rowcount  
            self._conn.commit()  
            return total_affected  
  
    def close(self):  
        """  
        Close the underlying JDBC connection.  
        """  
        if hasattr(self, '_conn') and self._conn:  
            self._conn.close()  
  
    def __enter__(self):  
        """Support for context manager usage."""  
        return self  
  
    def __exit__(self, exc_type, exc_val, exc_tb):  
        """Automatically close connection when exiting context."""  
        self.close()  


# Example usage for big spatial data
if __name__ == "__main__":  
    # Configuration for handling millions of rows
    config = QueryConfig(
        fetch_size=50000,        # Larger fetch size for big data
        max_memory_rows=200000,  # Higher memory limit
        parallel_chunks=8,       # More parallel processing
        commit_interval=10000    # Larger batch commits
    )
    
    with Database(config) as db:
        
        # Example 1: Stream processing millions of network nodes
        print("Processing 12M network nodes...")
        total_nodes = 0
        for chunk in db.query_stream(
            "SELECT node_id, ST_X(geom), ST_Y(geom) FROM network_nodes WHERE active = ?", 
            [True], 
            chunk_size=25000
        ):
            # Process each chunk of 25K nodes
            total_nodes += len(chunk)
            if total_nodes % 500000 == 0:
                print(f"Processed {total_nodes} nodes...")
        
        # Example 2: Parallel processing of network links by region
        def analyze_links_topology(rows):
            # Your network analysis logic here
            return {'link_count': len(rows), 'avg_length': sum(r[2] for r in rows) / len(rows)}
        
        results = db.query_parallel_chunks(
            "SELECT link_id, ST_Length(geom), region_id FROM network_links",
            "region_id",
            analyze_links_topology,
            num_partitions=12
        )
        
        # Example 3: Spatial query with geographic bounds
        nyc_bounds = (-74.25, 40.49, -73.70, 40.92)  # NYC bounding box
        for chunk in db.spatial_query_with_bounds(
            "SELECT equipment_id, name, ST_AsText(location) FROM equipment",
            "location",
            *nyc_bounds,
            chunk_size=10000
        ):
            # Process NYC equipment
            print(f"Processing {len(chunk)} NYC equipment records...")
        
        # Example 4: Bulk insert of new equipment with streaming
        def generate_equipment_stream():
            for i in range(200000):  # 200K new equipment
                yield [
                    f"EQ_{i:06d}", 
                    f"Equipment_{i}", 
                    f"POINT({-74 + (i % 100) * 0.001} {40.7 + (i % 100) * 0.001})"
                ]
        
        inserted = db.bulk_insert_stream(
            'equipment',
            ['equipment_id', 'name', 'location_wkt'],
            generate_equipment_stream(),
            batch_size=5000
        )
        print(f"Inserted {inserted} equipment records")
        
        # Example 5: Get table statistics
        stats = db.get_table_stats('network_nodes')
        print(f"Network nodes: {stats['row_count']:,} rows, size: {stats['table_size']}")
