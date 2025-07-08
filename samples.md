# JayDeBeApi Usage Samples

This document provides concise code examples for common database tasks using the `Database` wrapper from `db.py`.

---

## 1. Simple SELECT

Fetch all rows from a table:

```py
from db import Database

def fetch_users():
    db = Database()
    try:
        users = db.query(
            "SELECT id, username, email FROM users"
        )
        for uid, name, email in users:
            print(f"{uid}: {name} <{email}>")
    finally:
        db.close()
```

---

## 2. Parameterized Query

Avoid SQL injection by using placeholders:

```py
def get_active_tasks(status: bool):
    db = Database()
    try:
        sql = "SELECT task_id, title FROM tasks WHERE completed = ?"
        rows = db.query(sql, [1 if status else 0])
        return rows
    finally:
        db.close()
```

---

## 3. INSERT with Generated ID

Insert a row and retrieve the generated key:

```py
def add_user(username: str, email: str):
    db = Database()
    try:
        insert_sql = (
            "INSERT INTO users (username, email) VALUES (?, ?)"
        )
        count = db.update(insert_sql, [username, email])
        print(f"Inserted {count} row(s)")

        # If the driver supports getGeneratedKeys():
        with db.cursor() as cur:
            cur.execute(
                "SELECT LAST_INSERT_ID()"
            )
            new_id = cur.fetchone()[0]
        return new_id
    finally:
        db.close()
```

---

## 4. UPDATE and DELETE

Perform update and delete operations:

```py
# Update example
def mark_task_done(task_id: int):
    db = Database()
    try:
        sql = "UPDATE tasks SET completed = ? WHERE task_id = ?"
        updated = db.update(sql, [1, task_id])
        print(f"Updated {updated} task(s)")
    finally:
        db.close()

# Delete example
def delete_old_logs(cutoff_date: str):
    db = Database()
    try:
        sql = "DELETE FROM logs WHERE created_at < ?"
        deleted = db.update(sql, [cutoff_date])
        print(f"Deleted {deleted} log(s)")
    finally:
        db.close()
```

---

## 5. Batch Insert

Insert multiple rows efficiently:

```py
def bulk_insert_products(products: list[tuple[str, float]]):
    db = Database()
    try:
        insert_sql = (
            "INSERT INTO products (name, price) VALUES (?, ?)"
        )
        with db.cursor() as cur:
            cur.executemany(insert_sql, products)
            db._conn.commit()
        print(f"Inserted {len(products)} products")
    finally:
        db.close()
```

---

## 6. Stored Procedure

Call procedures with/without parameters:

```py
# No parameters
def refresh_views():
    db = Database()
    try:
        db.callproc("refresh_materialized_views")
        print("Views refreshed")
    finally:
        db.close()

# With parameters
def calculate_bonus(username: str, year: int):
    db = Database()
    try:
        db.callproc("calculate_bonus", [username, year])
        print("Bonus calculated")
    finally:
        db.close()
```

---

## 7. Transaction Example

Perform multiple operations atomically:

```py
from db import Database


def transfer_funds(from_id: int, to_id: int, amount: float):
    db = Database()
    conn = db._conn
    try:
        with db.cursor() as cur:
            cur.execute(
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                [amount, from_id]
            )
            cur.execute(
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                [amount, to_id]
            )
        conn.commit()
        print("Transfer completed")
    except Exception:
        conn.rollback()
        print("Transfer failed, rolled back")
    finally:
        db.close()
```

---

## 8. Handling Dates

Map `datetime.date` to JDBC-compatible string:

```py
import datetime
import jaydebeapi

# Register converter before connecting
jaydebeapi._jdbc.python2java[datetime.date] = lambda d: d.isoformat()

from db import Database

def insert_event(name: str, event_date: datetime.date):
    db = Database()
    try:
        sql = "INSERT INTO events (name, event_date) VALUES (?, ?)"
        db.update(sql, [name, event_date])
        print("Event inserted")
    finally:
        db.close()
```

---

## 9. LOB Handling

Insert and retrieve large text (CLOB):

```py
def insert_document(title: str, content: str):
    db = Database()
    try:
        sql = "INSERT INTO docs (title, content) VALUES (?, ?)"
        db.update(sql, [title, content])
        print("Document saved")
    finally:
        db.close()


def get_document(doc_id: int) -> str:
    db = Database()
    try:
        row = db.query(
            "SELECT content FROM docs WHERE id = ?", [doc_id]
        )
        return row[0][0] if row else ''
    finally:
        db.close()
```

---

## 10. Clean Shutdown

Ensure connections close on application exit:

```py
import atexit
from db import Database

db = Database()
atexit.register(db.close)
```

---

These samples should give you a solid foundation for most use cases. Customize them to match your schema and business logic.

