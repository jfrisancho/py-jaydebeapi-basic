Mdular, reusable script that splits each of your six ETL steps into its own method. All string literals use single quotes, only built-in types are used, and it leverages your Database class:
```py
#!/usr/bin/env python3
# load_utility_transitions.py

import re
from db import Database

# --- 0) your valid transitions constant ---
valid_transitions = {
    
}

def clean_label(label):
    '''Uppercase and strip all whitespace from a utility code.'''
    return re.sub(r'\s+', '', label).upper()

class UtilityTransitionLoader:
    def __init__(self, verbose=False):
        self.db = Database(verbose=verbose)
        self.unknown_labels = []

    def fetch_raw_utils(self):
        '''Step 1: Fetch raw utility_no + utility_code from tb_utilities.'''
        return self.db.query(
            'SELECT utility_no, utility_code FROM tb_utilities'
        )

    def build_code_map(self, raw_utils):
        '''Step 2: Build dict(clean_label → utility_no).'''
        code_map = {}
        for no, code in raw_utils:
            key = clean_label(code)
            code_map[key] = no
        return code_map

    def transform(self, code_map):
        '''Step 3: Turn valid_transitions into a deduped list of (src_no, tgt_no).'''
        pairs = []
        for src, targets in valid_transitions.items():
            src_no = code_map.get(clean_label(src))
            if src_no is None:
                self.unknown_labels.append(src)
                continue

            for tgt in targets:
                tgt_no = code_map.get(clean_label(tgt))
                if tgt_no is None:
                    self.unknown_labels.append(tgt)
                else:
                    pairs.append((src_no, tgt_no))

        # dedupe while preserving order
        seen = set()
        unique_pairs = []
        for p in pairs:
            if p not in seen:
                seen.add(p)
                unique_pairs.append(p)
        return unique_pairs

    def summarize_transformed(self, unique_pairs):
        '''Step 4: Print stats about transformation.'''
        total = sum(len(v) for v in valid_transitions.values())
        print(f'🟢 Raw transitions: {total}')
        print(f'🔄 After cleaning & dedupe: {len(unique_pairs)} unique pairs')
        if self.unknown_labels:
            u = sorted(set(self.unknown_labels))
            print(f'⚠️ Unknown labels: {u}')

    def fetch_existing(self):
        '''Step 5a: Get existing (src_no, tgt_no) from DB.'''
        rows = self.db.query(
            'SELECT source_utility_no, target_utility_no '
            'FROM tb_utility_transitions'
        )
        return set(rows)

    def filter_new(self, unique_pairs, existing):
        '''Step 5b: Return only those not already in DB.'''
        new = [p for p in unique_pairs if p not in existing]
        print(f'🔍 {len(new)} new pairs (skipping {len(unique_pairs)-len(new)} existing)')
        return new

    def insert(self, to_insert):
        '''Step 6a: Bulk-insert with execute_batch.'''
        if not to_insert:
            print('ℹ️ Nothing to insert.')
            return 0

        sql = (
            'INSERT INTO tb_utility_transitions '
            '(source_utility_no, target_utility_no) VALUES (?, ?)'
        )
        count = self.db.execute_batch(sql, to_insert)
        print(f'✅ Inserted {count} rows.')
        return count

    def final_stats(self):
        '''Step 6b: Fetch and print total count in DB.'''
        total = self.db.query('SELECT COUNT(*) FROM tb_utility_transitions')[0][0]
        print(f'📊 Total transitions now: {total}')

    def run(self):
        raw = self.fetch_raw_utils()
        code_map = self.build_code_map(raw)
        unique_pairs = self.transform(code_map)
        self.summarize_transformed(unique_pairs)

        existing = self.fetch_existing()
        new_pairs = self.filter_new(unique_pairs, existing)

        self.insert(new_pairs)
        self.final_stats()
        self.db.close()

if __name__ == '__main__':
    loader = UtilityTransitionLoader(verbose=True)
    loader.run()
```

Why this is better

Six methods matching your ETL steps.

clean_label centralizes cleanup logic.

transform returns deduped pairs; unknown labels are collected separately.

filter_new cleanly separates DB-check logic.

insert and final_stats each do one job.

Reusability: you can import UtilityTransitionLoader and call individual methods in tests or other workflows.

Single quotes and only built-in types (dict, list, tuple).


