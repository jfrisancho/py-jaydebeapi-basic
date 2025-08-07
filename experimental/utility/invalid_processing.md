Here’s a standalone Python script optimized for Python 3.11 that

1. parses your list of error strings with a single compiled regex


2. does one JDBC query to pull all no, label pairs into a dict


3. builds a result list of dicts with f_utility_no, t_utility_no, f_label and t_label



All string literals use single quotes, and only built-in generics (dict, list, tuple) are used:
```python
#!/usr/bin/env python3

import re
from db import Database

def process_transitions(transitions: list[str]) -> list[dict]:
    # compile once
    pattern = re.compile(r'Invalid utility transition from (\d+) to (\d+)')

    # extract all numeric pairs
    pairs: list[tuple[int, int]] = [
        (int(m.group(1)), int(m.group(2)))
        for line in transitions
        if (m := pattern.match(line))
    ]

    # connect and fetch all utility labels in one go
    db = Database()
    rows = db.query('select no, label from tb_utilities')
    label_map: dict[int, str] = {no: label for no, label in rows}

    # build final list
    result: list[dict] = [
        {
            'f_utility_no': f,
            't_utility_no': t,
            'f_label': label_map.get(f),
            't_label': label_map.get(t),
        }
        for f, t in pairs
    ]

    db.close()
    return result

if __name__ == '__main__':
    # example input
    transitions = [
        'Invalid utility transition from 135 to 182',
        'Invalid utility transition from 182 to 329',
        'Invalid utility transition from 329 to 266',
        # …
    ]

    for entry in process_transitions(transitions):
        print(entry)
```
Why this is efficient for large data (1M+ items):

One regex.compile call, reused for all lines

Single DB round-trip to fetch the (presumably small) tb_utilities table

List and dict comprehensions in C-speed loops, minimal Python overhead

Built-in generics (list, dict, tuple) with type hints for clarity but no runtime cost


Feel free to integrate this into your existing code or extend it (for example, to read the transitions list from a file or another source).

