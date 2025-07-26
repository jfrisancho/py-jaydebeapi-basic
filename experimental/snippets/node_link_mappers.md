Here’s the optimized code to print the value and type of the first 5 items from both node_id_mapper and link_id_mapper:
```python
def print_mapper_sample(mapper: dict[int, int], name: str, limit: int = 5):
    print(f"\n{name} sample ({limit} items):")
    for i, (k, v) in enumerate(mapper.items()):
        if i >= limit:
            break
        print(f"{i}: key = {k} ({type(k).__name__}), value = {v} ({type(v).__name__})")

print_mapper_sample(node_id_mapper, "node_id_mapper")
print_mapper_sample(link_id_mapper, "link_id_mapper")
```

---

# Output Example
```shell
node_id_mapper sample (5 items):
0: key = 101 (int), value = 0 (int)
1: key = 102 (int), value = 1 (int)
2: key = 103 (int), value = 2 (int)
3: key = 104 (int), value = 3 (int)
4: key = 105 (int), value = 4 (int)
```
