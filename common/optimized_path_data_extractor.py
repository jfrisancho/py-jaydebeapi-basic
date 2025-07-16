import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class PathData:
    """Data class to hold extracted path information"""
    unique_nodes: set[int]
    unique_links: set[int]
    total_cost: float
    total_length: float
    e2e_group_nos: list[int]
    data_codes: list[int]
    utility_nos: list[int]
    references: list[str]


def extract_markers(markers: str, only_label: bool = False) -> List[str]:
    """
    Extracts marker labels in the formats <PREFIX><SEQ>[(NO)] from a string.
    If only_label is True, returns just <PREFIX><SEQ> (e.g., 'X16' from 'X16(1)').
    Otherwise, returns the full marker including any (NO) (e.g., 'X16(1)').
    Handles both unformatted and formatted marker strings, including comma-separated lists.
    """
    # Match <PREFIX><SEQ> with optional (NO)
    pattern = r'([A-Za-z]+\d+)(\(\d+\))?'
    matches = re.findall(pattern, markers)
    if only_label:
        return [prefix for prefix, _ in matches]
    else:
        return [prefix + suffix for prefix, suffix in matches]


def get_path_data(cursor, path_id: int) -> Optional[PathData]:
    """
    Extracts comprehensive path data for a given path ID.
    
    Args:
        cursor: Database cursor object
        path_id: The ID of the path to extract data for
        
    Returns:
        PathData object containing all extracted information, or None if path not found
    """
    
    # Optimized query using JOIN to get all required data in one go
    query = """
    SELECT 
        pl.link_id,
        pl.length,
        pl.s_node_id,
        pl.e_node_id,
        pl.seq,
        nl.cost,
        sn.e2e_group_no as s_e2e_group_no,
        sn.data_code as s_data_code,
        sn.utility_no as s_utility_no,
        sn.markers as s_markers,
        en.e2e_group_no as e_e2e_group_no,
        en.data_code as e_data_code,
        en.utility_no as e_utility_no,
        en.markers as e_markers
    FROM tb_path_links pl
    JOIN nw_links nl ON pl.link_id = nl.id
    JOIN nw_nodes sn ON pl.s_node_id = sn.id
    JOIN nw_nodes en ON pl.e_node_id = en.id
    WHERE pl.path_id = ?
    ORDER BY pl.seq
    """
    
    cursor.execute(query, (path_id,))
    rows = cursor.fetchall()
    
    if not rows:
        return None
    
    # Initialize collections for efficient data gathering
    unique_nodes = set()
    unique_links = set()
    total_cost = 0.0
    total_length = 0.0
    e2e_groups = set()
    data_codes = set()
    utility_nos = set()
    references = set()
    
    # Process each row efficiently
    for row in rows:
        (link_id, length, s_node_id, e_node_id, seq, cost,
         s_e2e_group_no, s_data_code, s_utility_no, s_markers,
         e_e2e_group_no, e_data_code, e_utility_no, e_markers) = row
        
        # Collect unique nodes and links
        unique_links.add(link_id)
        unique_nodes.add(s_node_id)
        unique_nodes.add(e_node_id)
        
        # Accumulate costs and lengths
        total_cost += cost or 0.0
        total_length += length or 0.0
        
        # Collect e2e_group_nos
        e2e_groups.add(s_e2e_group_no)
        e2e_groups.add(e_e2e_group_no)
        
        # Collect data_codes
        data_codes.add(s_data_code)
        data_codes.add(e_data_code)
        
        # Collect utility_nos (handle None values)
        if s_utility_no is not None:
            utility_nos.add(s_utility_no)
        if e_utility_no is not None:
            utility_nos.add(e_utility_no)
        
        # Extract references from markers
        if s_markers:
            references.update(extract_markers(s_markers, only_label=False))
        if e_markers:
            references.update(extract_markers(e_markers, only_label=False))
    
    # Convert sets to sorted lists for consistent output
    return PathData(
        unique_nodes=unique_nodes,
        unique_links=unique_links,
        total_cost=total_cost,
        total_length=total_length,
        e2e_group_nos=sorted(e2e_groups),
        data_codes=sorted(data_codes),
        utility_nos=sorted(utility_nos),
        references=sorted(references)
    )


def get_path_data_batch(cursor, path_ids: List[int]) -> Dict[int, PathData]:
    """
    Extracts path data for multiple paths in a single database operation.
    More efficient for processing multiple paths.
    
    Args:
        cursor: Database cursor object
        path_ids: List of path IDs to extract data for
        
    Returns:
        Dictionary mapping path_id to PathData objects
    """
    if not path_ids:
        return {}
    
    # Create placeholder string for IN clause
    placeholders = ','.join('?' * len(path_ids))
    
    query = f"""
    SELECT 
        pl.path_id,
        pl.link_id,
        pl.length,
        pl.s_node_id,
        pl.e_node_id,
        pl.seq,
        nl.cost,
        sn.e2e_group_no as s_e2e_group_no,
        sn.data_code as s_data_code,
        sn.utility_no as s_utility_no,
        sn.markers as s_markers,
        en.e2e_group_no as e_e2e_group_no,
        en.data_code as e_data_code,
        en.utility_no as e_utility_no,
        en.markers as e_markers
    FROM tb_path_links pl
    JOIN nw_links nl ON pl.link_id = nl.id
    JOIN nw_nodes sn ON pl.s_node_id = sn.id
    JOIN nw_nodes en ON pl.e_node_id = en.id
    WHERE pl.path_id IN ({placeholders})
    ORDER BY pl.path_id, pl.seq
    """
    
    cursor.execute(query, path_ids)
    rows = cursor.fetchall()
    
    # Group data by path_id
    path_data_dict = defaultdict(lambda: {
        'unique_nodes': set(),
        'unique_links': set(),
        'total_cost': 0.0,
        'total_length': 0.0,
        'e2e_groups': set(),
        'data_codes': set(),
        'utility_nos': set(),
        'references': set()
    })
    
    # Process all rows
    for row in rows:
        (path_id, link_id, length, s_node_id, e_node_id, seq, cost,
         s_e2e_group_no, s_data_code, s_utility_no, s_markers,
         e_e2e_group_no, e_data_code, e_utility_no, e_markers) = row
        
        data = path_data_dict[path_id]
        
        # Collect unique nodes and links
        data['unique_links'].add(link_id)
        data['unique_nodes'].add(s_node_id)
        data['unique_nodes'].add(e_node_id)
        
        # Accumulate costs and lengths
        data['total_cost'] += cost or 0.0
        data['total_length'] += length or 0.0
        
        # Collect e2e_group_nos
        data['e2e_groups'].add(s_e2e_group_no)
        data['e2e_groups'].add(e_e2e_group_no)
        
        # Collect data_codes
        data['data_codes'].add(s_data_code)
        data['data_codes'].add(e_data_code)
        
        # Collect utility_nos (handle None values)
        if s_utility_no is not None:
            data['utility_nos'].add(s_utility_no)
        if e_utility_no is not None:
            data['utility_nos'].add(e_utility_no)
        
        # Extract references from markers
        if s_markers:
            data['references'].update(extract_markers(s_markers, only_label=False))
        if e_markers:
            data['references'].update(extract_markers(e_markers, only_label=False))
    
    # Convert to PathData objects
    result = {}
    for path_id in path_ids:
        if path_id in path_data_dict:
            data = path_data_dict[path_id]
            result[path_id] = PathData(
                unique_nodes=data['unique_nodes'],
                unique_links=data['unique_links'],
                total_cost=data['total_cost'],
                total_length=data['total_length'],
                e2e_group_nos=sorted(data['e2e_groups']),
                data_codes=sorted(data['data_codes']),
                utility_nos=sorted(data['utility_nos']),
                references=sorted(data['references'])
            )
    
    return result


# Example usage:
"""
# For single path
path_data = get_path_data(cursor, 123)
if path_data:
    print(f"Unique nodes: {len(path_data.unique_nodes)}")
    print(f"Unique links: {len(path_data.unique_links)}")
    print(f"Total cost: {path_data.total_cost}")
    print(f"Total length: {path_data.total_length}")
    print(f"E2E groups: {path_data.e2e_group_nos}")
    print(f"Data codes: {path_data.data_codes}")
    print(f"Utility numbers: {path_data.utility_nos}")
    print(f"References: {path_data.references}")

# For multiple paths (more efficient)
batch_data = get_path_data_batch(cursor, [123, 456, 789])
for path_id, data in batch_data.items():
    print(f"Path {path_id}: {len(data.unique_nodes)} nodes, {len(data.unique_links)} links")
"""
