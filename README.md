# JayDeBeApi Basic Guide

This guide demonstrates how to use [JayDeBeApi](https://pypi.org/project/JayDeBeApi/) for JDBC connectivity in Python, leveraging a wrapper defined in `db.py`. It covers installation, configuration, and common operations (SELECT, INSERT/UPDATE/DELETE, stored procedures) with code snippets to help you bootstrap any project.

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [The `Database` Wrapper](#the-database-wrapper)
  - [Connecting](#connecting)
  - [Context-Managed Cursors](#context-managed-cursors)
  - [Querying Data](#querying-data)
  - [Modifying Data](#modifying-data)
  - [Calling Stored Procedures](#calling-stored-procedures)
  - [Closing the Connection](#closing-the-connection)
- [Error Handling & Best Practices](#error-handling--best-practices)
- [Putting It All Together](#putting-it-all-together)

---

## Prerequisites

- Python 3.6+
- Java Runtime Environment (JRE) installed and on your `PATH`
- JDBC driver `.jar` file for your database (e.g., PostgreSQL, Oracle, SQL Server)

## Installation

Install JayDeBeApi via pip:

```bash
pip install JayDeBeApi
```

## Configuration

Create a `config.py` file to centralize your JDBC settings:

```py
# config.py
JDBC_URL = "jdbc:postgresql://localhost:5432/mydb"
DB_USER = "myuser"
DB_PASSWORD = "mypassword"
DRIVER_CLASS = "org.postgresql.Driver"
DRIVER_PATH = "/path/to/postgresql-42.2.5.jar"
```

Adjust the constants to match your environment and database.

---

## The `Database` Wrapper

`db.py` provides a `Database` class that wraps JayDeBeApi connections and offers high-level methods for common operations.

```py
from contextlib import contextmanager
import jaydebeapi
from config import JDBC_URL, DB_USER, DB_PASSWORD, DRIVER_CLASS, DRIVER_PATH

class Database:
    def __init__(self):
        self._conn = jaydebeapi.connect(
            DRIVER_CLASS,
            JDBC_URL,
            [DB_USER, DB_PASSWORD],
            DRIVER_PATH
        )

    @contextmanager
    def cursor(self):
        cur = None
        try:
            cur = self._conn.cursor()
            yield cur
        finally:
            if cur:
                cur.close()

    def query(self, sql: str, params: list = None) -> list:
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return cur.fetchall()

    def update(self, sql: str, params: list = None) -> int:
        with self.cursor() as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            self._conn.commit()
            return cur.rowcount

    def callproc(self, proc_name: str, params: list = None):
        with self.cursor() as cur:
            cur.callproc(proc_name, params or [])
            self._conn.commit()

    def close(self):
        if self._conn:
            self._conn.close()
```

### Connecting

Instantiate the `Database` class to open a JDBC connection:

```py
from db import Database

db = Database()
```

### Context-Managed Cursors

Use `with db.cursor() as cur:` to ensure cursors are closed automatically:

```py
with db.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchall())
```

### Querying Data

Use `db.query(...)` to run a `SELECT` and fetch all rows:

```py
users = db.query(
    "SELECT id, username, email FROM users WHERE active = ?",
    [1]
)
for user in users:
    print(user)
```

### Modifying Data

Use `db.update(...)` for `INSERT`, `UPDATE`, or `DELETE`. It returns the number of affected rows:

```py
rows_inserted = db.update(
    "INSERT INTO tasks (title, due_date) VALUES (?, ?)",
    ["Write guide", "2025-07-15"]
)
print(f"Inserted {rows_inserted} task(s)")
```

### Calling Stored Procedures

Use `db.callproc(...)` to invoke stored procedures:

```py
# No parameters
db.callproc("refresh_materialized_views")

# With parameters
db.callproc(
    "calculate_bonus",
    ["john_doe", 2025]
)
```

### Closing the Connection

Always close the connection when done:

```py
db.close()
```

---

## Error Handling & Best Practices

- **Handle connection errors**: Wrap `Database()` instantiation in `try/except`.
- **Use parameterized queries**: Avoid SQL injection by never concatenating raw strings.
- **Commit only when needed**: `query` is read-only; `update` and `callproc` commit.
- **Manage long-running transactions**: Keep transactions short to avoid locks.

```py
try:
    db = Database()
except RuntimeError as e:
    print(f"Connection failed: {e}")
    exit(1)
```

---

## Putting It All Together

Here's a minimal example script `app.py`:

```py
# app.py
from db import Database

def main():
    db = Database()
    try:
        # Fetch active users
        users = db.query(
            "SELECT id, username FROM users WHERE active = ?", [1]
        )
        for uid, name in users:
            print(f"User {uid}: {name}")

        # Insert a new record
        count = db.update(
            "INSERT INTO logs (message) VALUES (?)", ["Started main process"]
        )
        print(f"Logged {count} message(s)")

        # Call a proc
        db.callproc("send_notifications")
    finally:
        db.close()

if __name__ == '__main__':
    main()
```

With this pattern, you have a robust starting point for any JDBC-backed Python project.

---

Happy coding! Feel free to extend `Database` with additional helpers (batch inserts, connection pools, etc.) as your project grows.

