from itertools import islice
import jaydebeapi

class Database:
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
