from itertools import islice
import math
import jaydebeapi

class Database:
    # …

    def executemany_batch(
        self,
        sql: str,
        params_list: list[list[object]],
        chunk_size: int = 1000
    ) -> int:
        """
        Execute the same SQL statement multiple times with different parameters,
        in batches for efficiency, and print progress per chunk.

        :param sql:          SQL statement with '?' placeholders
        :param params_list:  List of parameter sequences
        :param chunk_size:   Number of rows to send per batch
        :returns:            Total number of rows “sent” (i.e. len(params_list))
        """
        def _chunked(it, size):
            it = iter(it)
            while batch := list(islice(it, size)):
                yield batch

        total_rows = len(params_list)
        total_chunks = math.ceil(total_rows / chunk_size)
        printed = not getattr(self, "silence", False)

        total = 0
        try:
            with self.cursor() as cur:
                for idx, batch in enumerate(_chunked(params_list, chunk_size), start=1):
                    cur.executemany(sql, batch)
                    total += len(batch)

                    if printed:
                        print(f"[{idx}/{total_chunks}] chunk inserted {len(batch)} rows")

            self._conn.commit()
        except jaydebeapi.DatabaseError as err:
            if printed:
                print(f"    x Bulk insert failed on chunk {idx}: {err}")
            raise

        if printed:
            print(f"Done — {total} rows in {total_chunks} chunks.")
        return total
    # …

    def execute_bash(
        self,
        sql: str,
        params_list: list[list[object]],
        chunk_size: int = 1000
    ) -> int:
        """
        Execute the same SQL statement multiple times with different parameters,
        in batches for efficiency.

        :param sql:          SQL statement with '?' placeholders
        :param params_list:  List of parameter sequences
        :param chunk_size:   Number of rows to send per batch to cursor.executemany
        :returns:            Total number of rows “sent” (i.e. len(params_list))
        """
        def _chunked(it, size):
            it = iter(it)
            while batch := list(islice(it, size)):
                yield batch

        total = 0
        try:
            with self.cursor() as cur:
                for batch in _chunked(params_list, chunk_size):
                    # one round‐trip + bind per batch
                    cur.executemany(sql, batch)
                    total += len(batch)
            self._conn.commit()
        except jaydebeapi.DatabaseError as err:
            # if silence is False, log the failure
            if not getattr(self, "silence", False):
                print(f"    x Bulk insert failed: {err}")
            # re‐raise so caller knows something went wrong
            raise
        return total


def execute_batch(self, sql: str, params_list: list[list], 
                  batch_size: int = 1000, 
                  commit_frequency: int = 1000) -> int:
    """
    Execute the same SQL statement multiple times with different parameters.
    Optimized for large datasets without using executemany.

    Args:
        sql: SQL statement with placeholders
        params_list: List of parameter lists
        batch_size: Number of records to process before checking memory/performance
        commit_frequency: How often to commit (affects transaction size)

    Returns:
        Total number of affected rows

    Example:
        # Batch insert 10,000 records
        sql = "INSERT INTO users (name, email) VALUES (?, ?)"
        params = [['User1', 'user1@example.com'], ['User2', 'user2@example.com']] * 5000
        affected_rows = db.execute_batch(sql, params)
    """
    if not params_list:
        return 0

    total_affected = 0
    processed_count = 0
    
    # Single cursor for the entire operation (more efficient)
    with self.cursor() as cur:
        try:
            # Prepare statement once (if database supports it)
            if hasattr(cur, 'prepare'):
                cur.prepare(sql)
            
            for i, params in enumerate(params_list):
                cur.execute(sql, params)
                total_affected += cur.rowcount
                processed_count += 1
                
                # Commit periodically to avoid long transactions
                if processed_count % commit_frequency == 0:
                    self._conn.commit()
                    if not self.silence:
                        print(f'    ✓ Committed batch: {processed_count}/{len(params_list)} records processed')
                
                # Optional: Yield control periodically for very large datasets
                if processed_count % batch_size == 0:
                    pass  # Could add time.sleep(0) here if needed
            
            # Final commit for remaining records
            if processed_count % commit_frequency != 0:
                self._conn.commit()
                
            if not self.silence:
                print(f'    ✓ Completed: {total_affected} total records affected')
                
        except Exception as err:
            self._conn.rollback()
            if not self.silence:
                print(f'    ✗ Failed at record {processed_count + 1}: {err}')
            raise

    return total_affected


def execute_batch_chunked(self, sql: str, params_list: list[list], 
                         chunk_size: int = 1000) -> int:
    """
    Process large batches in chunks with separate transactions.
    Better for very large datasets as it prevents memory issues and lock timeouts.
    
    Args:
        sql: SQL statement with placeholders  
        params_list: List of parameter lists
        chunk_size: Number of records per transaction chunk
        
    Returns:
        Total number of affected rows
    """
    if not params_list:
        return 0
    
    total_affected = 0
    total_chunks = (len(params_list) + chunk_size - 1) // chunk_size
    
    for chunk_num in range(0, len(params_list), chunk_size):
        chunk = params_list[chunk_num:chunk_num + chunk_size]
        current_chunk = chunk_num // chunk_size + 1
        
        try:
            chunk_affected = self._execute_chunk(sql, chunk)
            total_affected += chunk_affected
            
            if not self.silence:
                print(f'    ✓ Chunk {current_chunk}/{total_chunks}: {len(chunk)} records, {chunk_affected} affected')
                
        except Exception as err:
            if not self.silence:
                print(f'    ✗ Failed chunk {current_chunk}/{total_chunks}: {err}')
            raise
    
    return total_affected


def _execute_chunk(self, sql: str, params_chunk: list[list]) -> int:
    """Execute a single chunk of parameters."""
    chunk_affected = 0
    
    with self.cursor() as cur:
        try:
            # Prepare statement once per chunk
            if hasattr(cur, 'prepare'):
                cur.prepare(sql)
                
            for params in params_chunk:
                cur.execute(sql, params)
                chunk_affected += cur.rowcount
            
            self._conn.commit()
            
        except Exception as err:
            self._conn.rollback()
            raise
            
    return chunk_affected


def execute_batch_bulk_values(self, table_name: str, columns: list[str], 
                             values_list: list[list], chunk_size: int = 500) -> int:
    """
    Optimized bulk insert using multi-row VALUES syntax.
    Much faster than individual inserts for INSERT operations.
    
    Args:
        table_name: Name of target table
        columns: List of column names
        values_list: List of value lists
        chunk_size: Number of rows per bulk insert (keep lower for complex data)
        
    Returns:
        Total number of affected rows
        
    Example:
        # Instead of individual INSERT statements
        affected = db.execute_batch_bulk_values(
            'users', 
            ['name', 'email'], 
            [['John', 'john@example.com'], ['Jane', 'jane@example.com']] * 5000
        )
    """
    if not values_list:
        return 0
        
    total_affected = 0
    columns_str = ', '.join(columns)
    placeholder_row = '(' + ', '.join(['?'] * len(columns)) + ')'
    
    for i in range(0, len(values_list), chunk_size):
        chunk = values_list[i:i + chunk_size]
        
        # Create multi-row VALUES clause
        values_placeholders = ', '.join([placeholder_row] * len(chunk))
        bulk_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_placeholders}"
        
        # Flatten the parameters
        flat_params = [value for row in chunk for value in row]
        
        with self.cursor() as cur:
            try:
                cur.execute(bulk_sql, flat_params)
                chunk_affected = cur.rowcount
                self._conn.commit()
                total_affected += chunk_affected
                
                if not self.silence:
                    print(f'    ✓ Bulk inserted: {len(chunk)} records')
                    
            except Exception as err:
                self._conn.rollback()
                if not self.silence:
                    print(f'    ✗ Bulk insert failed: {err}')
                raise
    
    return total_affected


def execute_batch_memory_efficient(self, sql: str, params_generator, 
                                 commit_frequency: int = 1000) -> int:
    """
    Memory-efficient batch execution using generators.
    Perfect for processing huge datasets that don't fit in memory.
    
    Args:
        sql: SQL statement with placeholders
        params_generator: Generator or iterator yielding parameter lists
        commit_frequency: How often to commit transactions
        
    Returns:
        Total number of affected rows
        
    Example:
        def generate_users():
            for i in range(100000):
                yield [f'User{i}', f'user{i}@example.com']
                
        affected = db.execute_batch_memory_efficient(
            "INSERT INTO users (name, email) VALUES (?, ?)",
            generate_users()
        )
    """
    total_affected = 0
    processed_count = 0
    
    with self.cursor() as cur:
        try:
            # Prepare statement once if supported
            if hasattr(cur, 'prepare'):
                cur.prepare(sql)
            
            for params in params_generator:
                cur.execute(sql, params)
                total_affected += cur.rowcount
                processed_count += 1
                
                # Periodic commits
                if processed_count % commit_frequency == 0:
                    self._conn.commit()
                    if not self.silence:
                        print(f'    ✓ Processed {processed_count} records')
            
            # Final commit
            if processed_count % commit_frequency != 0:
                self._conn.commit()
                
            if not self.silence:
                print(f'    ✓ Total processed: {processed_count} records, {total_affected} affected')
                
        except Exception as err:
            self._conn.rollback()
            if not self.silence:
                print(f'    ✗ Failed at record {processed_count + 1}: {err}')
            raise
    
    return total_affected


# Utility function to estimate optimal batch size
def estimate_optimal_batch_size(self, sample_sql: str, sample_params: list) -> int:
    """
    Estimate optimal batch size based on execution timing.
    Run this once to find the best chunk size for your specific use case.
    """
    import time
    
    test_sizes = [100, 500, 1000, 2000, 5000]
    best_size = 1000
    best_rate = 0
    
    if not self.silence:
        print("Estimating optimal batch size...")
    
    for size in test_sizes:
        if len(sample_params) < size:
            continue
            
        test_params = sample_params[:size]
        start_time = time.time()
        
        try:
            self.execute_batch(sample_sql, test_params, commit_frequency=size)
            elapsed = time.time() - start_time
            rate = size / elapsed  # records per second
            
            if rate > best_rate:
                best_rate = rate
                best_size = size
                
            if not self.silence:
                print(f"    Batch size {size}: {rate:.1f} records/sec")
                
        except Exception:
            continue
    
    if not self.silence:
        print(f"    Recommended batch size: {best_size}")
    
    return best_size
