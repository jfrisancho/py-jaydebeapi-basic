Below is an alternative that never builds a giant placeholder list, pushes all the work into the database engine, and streams back rows in manageable batches. We can try both the **EXISTS** approach and the **JOIN+UNION** approach and benchmark which our JDBC driver optimizes best.

---

## 1. We need a streaming cursor

(If We haven’t already, add this to our `Database` class.)

```python
from typing import Iterator, Any, Optional

class Database:
    # … our existing methods …

    def stream_query(
        self,
        sql: str,
        params: Optional[list[Any]] = None,
        fetch_size: int = 5_000,
    ) -> Iterator[tuple]:
        cur = self._conn.cursor()
        try:
            # hint to fetch in chunks
            try:
                cur._cursor.setFetchSize(fetch_size)
            except Exception:
                cur.arraysize = fetch_size

            cur.execute(sql, params or [])
            while batch := cur.fetchmany(fetch_size):
                for row in batch:
                    yield row
        finally:
            cur.close()
```

---

## 2. EXISTS‐based query

```sql
SELECT l.link_id AS id
FROM nw_link l
WHERE EXISTS (
  SELECT 1
  FROM nw_node n
  LEFT JOIN org_shape sh
    ON sh.node_id = n.node_id
  /* our same {where_clause}, e.g.
     WHERE n.fab_no = ? AND sh.phase_no = ? AND … */
  AND n.node_id IN (l.start_node_id, l.end_node_id)
)
ORDER BY l.link_id
```

**Why?**

* We never materialize millions of node IDs client-side
* The DB uses its native indexes on `n.node_id`, `l.start_node_id`, and `l.end_node_id` to short-circuit each link

---

## 3. JOIN + UNION approach

Some engines optimize a straight join better than EXISTS:

```sql
WITH filtered_nodes AS (
  SELECT n.node_id
  FROM nw_node n
  LEFT JOIN org_shape sh
    ON sh.node_id = n.node_id
  /* our {where_clause} here */
)
SELECT DISTINCT l.link_id AS id
FROM nw_link l
JOIN filtered_nodes fn
  ON fn.node_id = l.start_node_id
UNION
SELECT DISTINCT l.link_id AS id
FROM nw_link l
JOIN filtered_nodes fn
  ON fn.node_id = l.end_node_id
ORDER BY id
```

---

## 4. Pulling the IDs in Python

```python
# 1) Build our filters & where_clause via StringHelper
where_clause, params = StringHelper.build_where_clause(filters)

# 2) Plug them into our chosen SQL
sql = """SELECT l.link_id AS id
FROM nw_link l
WHERE EXISTS (
  SELECT 1
  FROM nw_node n
  LEFT JOIN org_shape sh ON sh.node_id = n.node_id
  {where_clause}
  AND n.node_id IN (l.start_node_id, l.end_node_id)
)
ORDER BY l.link_id""".format(where_clause=where_clause)

# 3) Stream & collect
link_ids = [row[0] for row in db.stream_query(sql, params, fetch_size=10_000)]
print(f"Total link IDs in scope: {len(link_ids)}")
```

---

### Why this will scale

1. **Single SQL round-trip** — We only send a handful of filter parameters, not millions.
2. **DB does the heavy lifting** — joins, indexes, UNION/EXISTS are executed in optimized C, not Python loops.
3. **Low Python memory footprint** — We only hold one batch of 10 000 rows in RAM at a time while fetching.

Give both variants a try, benchmark with `time.perf_counter_ns()`, and pick the one our database optimizer likes best.
