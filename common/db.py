import jaydebeapi
from contextlib import contextmanager
from typing import List, Optional, Dict, Any, Union
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH

class Database:
    """
    Encapsulates a single JDBC connection. Provides context-manager
    cursors and high-level methods for SELECT/INSERT/UPDATE/DELETE
    and stored-procedure calls. Does NOT enforce a singleton—caller
    is responsible for instantiating exactly one or more as needed.
    """

    def __init__(self):
        """
        Open the JDBC connection upon instantiation.
        """
        try:
            self._conn = jaydebeapi.connect(
                DRIVER_CLASS,
                JDBC_URL,
                [DB_USER, DB_PASSWORD],
                DRIVER_PATH
            )
        except jaydebeapi.DatabaseError as e:
            raise RuntimeError(f"Failed to connect via JDBC: {e}")

    @contextmanager
    def cursor(self):
        """
        Provide a cursor as a context manager, so it automatically
        closes even if exceptions happen.
        Usage:
            with db.cursor() as cur:
                cur.execute(SQL, params)
                rows = cur.fetchall()
        """
        cur = None
        try:
            cur = self._conn.cursor()
            yield cur
        finally:
            if cur:
                cur.close()

    def query(self, sql: str, params: Optional[List] = None) -> List:
        """
        Execute a SELECT statement and return all rows.
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
        
        Args:
            proc_name: Name of the stored procedure
            params: List of input parameters (optional)
            
        Returns:
            Result set if the procedure returns data, None otherwise
            
        Examples:
            # Procedure with no parameters
            db.callproc('GetAllUsers')
            
            # Procedure with input parameters
            db.callproc('GetUserById', [123])
            
            # Procedure with mixed parameters
            db.callproc('UpdateUserStatus', [123, 'active'])
        """
        with self.cursor() as cur:
            if params:
                # Create placeholders for parameters
                placeholders = ','.join(['?' for _ in params])
                call_sql = f"CALL {proc_name}({placeholders})"
                cur.execute(call_sql, params)
            else:
                # No parameters
                call_sql = f"CALL {proc_name}()"
                cur.execute(call_sql)
            
            self._conn.commit()
            
            # Try to fetch results if the procedure returns data
            try:
                return cur.fetchall()
            except:
                # Some procedures don't return data, that's fine
                return None

    def callproc_with_output(self, proc_name: str, in_params: Optional[List] = None, 
                           out_param_count: int = 0) -> Dict[str, Any]:
        """
        Call a stored procedure that has output parameters.
        
        Note: JDBC output parameter handling varies by database type.
        This is a generic implementation that may need database-specific adjustments.
        
        Args:
            proc_name: Name of the stored procedure
            in_params: List of input parameters
            out_param_count: Number of output parameters expected
            
        Returns:
            Dictionary with 'result_set' and 'output_params' keys
            
        Examples:
            # Procedure with input and output parameters
            result = db.callproc_with_output('GetUserStats', [123], out_param_count=2)
            print(result['result_set'])  # Any returned rows
            print(result['output_params'])  # Output parameter values
        """
        with self.cursor() as cur:
            total_params = (len(in_params) if in_params else 0) + out_param_count
            
            if total_params > 0:
                # Create placeholders for all parameters (input + output)
                placeholders = ','.join(['?' for _ in range(total_params)])
                call_sql = f"CALL {proc_name}({placeholders})"
                
                # Prepare all parameters (input params + None for output params)
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
            
            # Try to fetch result set
            try:
                result['result_set'] = cur.fetchall()
            except:
                pass
            
            # Note: Getting output parameter values depends on the specific JDBC driver
            # This is a placeholder - you may need to implement database-specific logic
            # For example, some databases support cursor.getMoreResults() or similar
            
            return result

    def call_function(self, func_name: str, params: Optional[List] = None) -> Any:
        """
        Call a database function (returns a single value).
        
        Args:
            func_name: Name of the function
            params: List of parameters
            
        Returns:
            The function's return value
            
        Examples:
            # Function with parameters
            result = db.call_function('CalculateTotal', [100, 0.15])
            
            # Function without parameters
            current_time = db.call_function('GetCurrentTime')
        """
        with self.cursor() as cur:
            if params:
                placeholders = ','.join(['?' for _ in params])
                # Most databases use SELECT for function calls
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
        More efficient than multiple individual execute calls.
        
        Args:
            sql: SQL statement with placeholders
            params_list: List of parameter lists
            
        Returns:
            Total number of affected rows
            
        Example:
            # Batch insert
            sql = "INSERT INTO users (name, email) VALUES (?, ?)"
            params = [
                ['John', 'john@example.com'],
                ['Jane', 'jane@example.com']
            ]
            db.execute_batch(sql, params)
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


# Example usage and testing
if __name__ == "__main__":
    # Example usage patterns
    
    # Using as context manager (recommended)
    with Database() as db:
        # Simple procedure call
        db.callproc('RefreshMaterializedViews')
        
        # Procedure with parameters
        users = db.callproc('GetUsersByDepartment', ['Engineering'])
        
        # Function call
        total = db.call_function('CalculateMonthlyRevenue', [2024, 1])
        
        # Query with results
        results = db.query("SELECT * FROM users WHERE active = ?", [True])
    
    # Or traditional usage
    db = Database()
    try:
        # Your database operations here
        pass
    finally:
        db.close()
