import jaydebeapi
from jaydebeapi import _DEFAULT_CONVERTERS, _java_to_py
from contextlib import contextmanager
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH

def _bit_to_bool(value):
    """
    Convert BIT(1) byte array to boolean.
    BIT(1) typically returns a byte array where b'\x01' = True, b'\x00' = False
    """
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return bool(value[0]) if len(value) > 0 else False
    return bool(value)

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
        # Fix typos and add BIT converter
        _DEFAULT_CONVERTERS.update({'BIGINT': _java_to_py('longValue')})
        _DEFAULT_CONVERTERS.update({'CLOB': _java_to_py('toString')})
        _DEFAULT_CONVERTERS.update({'DATE': _java_to_py('toString')})
        _DEFAULT_CONVERTERS.update({'BIT': _bit_to_bool})
        
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

    def query(self, sql: str, params: list = None) -> list:
        """
        Execute a SELECT statement and return all rows.
        """
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            try:
                return cur.fetchall()
            except jaydebeapi.Error as err:
                print(f'Failed to execute query: {err}')
                raise

    def update(self, sql: str, params: list = None) -> int:
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

    def callproc(self, proc_name: str, params: list = None):
        """
        Call a stored procedure. If params is None, calls without arguments.
        """
        with self.cursor() as cur:
            if params:
                cur.callproc(proc_name, params)
            else:
                cur.callproc(proc_name, [])
            self._conn.commit()

    def close(self):
        """
        Close the underlying JDBC connection.
        """
        if hasattr(self, '_conn') and self._conn:
            self._conn.close()
