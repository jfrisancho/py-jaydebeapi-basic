# Common Solutions for JayDeBeApi Errors and Pitfalls (v1.2)

This document fixes common JayDeBeApi issues—optimized for Python 3.11 and JayDeBeApi 1.2.3—without relying on private imports.

---

## 1. JDBC Type Conversion

### 1.1 Mapping `BIGINT`, `INTEGER`, `DOUBLE` to Python primitives

**Problem:** JayDeBeApi returns Java wrapper objects (e.g., `java.lang.Long`) instead of Python `int` or `float`.

**Solution:** Use the public `converters` registry to map JDBC types to Python callables:

```py
import jaydebeapi

# Helper: wrap a Java getter method into a Python callable
def java_getter(method_name: str):
    return lambda java_obj: getattr(java_obj, method_name)()

# Register converters before connecting
jaydebeapi.converters.update({
    'BIGINT':  java_getter('longValue'),    # maps java.lang.Long -> int
    'INTEGER': java_getter('intValue'),     # maps java.lang.Integer -> int
    'DOUBLE':  java_getter('doubleValue'),  # maps java.lang.Double -> float
})

# Proceed to import and use Database
from db import Database
```

---

## 2. Date & Time Handling

### 2.1 Inserting `datetime.date` and `datetime.datetime`

**Problem:** Inserting Python `date` or `datetime` raises `SQLException: Wrong input value`.

**Solution:** Use the public `python2jdbc` registry to convert Python objects into JDBC-compatible strings:

```py
import datetime
import jaydebeapi

# Register Python-to-JDBC converters
jaydebeapi.python2jdbc[datetime.date] = lambda d: d.isoformat()
jaydebeapi.python2jdbc[datetime.datetime] = lambda dt: dt.strftime('%Y-%m-%d %H:%M:%S')

from db import Database
```

---

## 3. Placeholder Syntax

**Issue:** Different drivers expect different parameter markers.

**Guideline:** Always use `?` placeholders with JayDeBeApi; most JDBC drivers support this natively:

```py
users = db.query("SELECT * FROM users WHERE id = ?", [user_id])
```

Avoid Python-style `%s` formatting or f-string interpolation to prevent SQL injection.

---

## 4. Batch Operations & Commits

**Problem:** `executemany()` doesn’t auto-commit, so inserts/updates aren’t persisted.

**Solution:** Commit explicitly after batch operations:

```py
with db.cursor() as cur:
    cur.executemany(
        "INSERT INTO items (name, qty) VALUES (?, ?)",
        item_list
    )
# Explicit commit on the connection
db._conn.commit()
```

---

## 5. Stored Procedure Nuances

- **No resultset:** Some drivers need you to call `fetchall()` even when no rows are returned.
- **OUT parameters:** Not all drivers support retrieving OUT parameters.

```py
with db.cursor() as cur:
    cur.callproc('my_proc', [in1, in2])
    try:
        rows = cur.fetchall()
    except jaydebeapi.DatabaseError:
        rows = []
```

---

## 6. Handling LOBs (CLOB/BLOB)

**Problem:** Large objects can be inefficient or fail without size hints.

**Solution:** If supported, set input sizes or stream:

```py
with db.cursor() as cur:
    # Hint at input size for a CLOB
    cur.setinputsizes([(None, len(large_text))])
    cur.execute(
        "INSERT INTO documents (doc_id, content) VALUES (?, ?)",
        [doc_id, large_text]
    )
    db._conn.commit()
```

For BLOB streaming, refer to your driver’s documentation.

---

## 7. Character Encoding

**Problem:** Non-ASCII data appears garbled.

**Solution:** Pass a `properties` dict when connecting:

```py
props = {
    'useUnicode': 'true',
    'characterEncoding': 'UTF-8',
}
db = jaydebeapi.connect(
    DRIVER_CLASS,
    JDBC_URL,
    [DB_USER, DB_PASSWORD],
    DRIVER_PATH,
    props
)
```

---

## 8. Connection Resilience

**Problem:** Idle or long-running jobs lose JDBC connections.

**Solution:** Wrap work in retry logic:

```py
from time import sleep
from db import Database

while True:
    try:
        db = Database()
        do_work(db)
    except jaydebeapi.DatabaseError as e:
        print(f"Connection lost: {e}, retrying in 5s...")
        sleep(5)
        continue
    finally:
        db.close()
    break
```

---

## 9. Thread Safety

- **Don’t share** one `Database` instance across threads.
- Create and close a separate `Database()` per thread.

```py
def worker():
    db = Database()
    try:
        process(db)
    finally:
        db.close()
```

---

## 10. Debug & Logging

Enable JDBC or driver-specific logging:

```py
import os
os.environ['JAVA_TOOL_OPTIONS'] = (
    '-Djava.util.logging.config.file=/path/to/logging.properties'
)
```

Review logs to trace SQL, connection, or driver issues.

---

*Version 1.2 – Updated to remove private imports and align with JayDeBeApi 1.2.3.*

