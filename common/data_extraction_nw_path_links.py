import re
from typing import Any, Dict, List, Set

def extract_markers(markers: str, only_label: bool = False) -> List[str]:
    """
    Extracts marker labels in the formats <PREFIX><SEQ>[(NO)] from a string.
    If only_label is True, returns just <PREFIX><SEQ> (e.g., 'X16').
    Otherwise, returns the full marker including any (NO) (e.g., 'X16(1)').
    """
    pattern = r'([A-Za-z]+\d+)(\(\d+\))?'
    matches = re.findall(pattern, markers)
    return [prefix for prefix, _ in matches] if only_label else [prefix + suffix for prefix, suffix in matches]

def get_path_info(conn: Any, path_id: int) -> Dict[str, Any]:
    """
    Fetches and aggregates tb_path_links + nw_links + nw_nodes data for a given path_id.
    
    Returns a dict with:
      - unique_nodes, unique_links
      - total_cost, total_length
      - e2e_group_nos, data_codes, utility_nos
      - references (from markers)
    """
    # 1) Pull all path‐link rows (with link cost)
    sql_links = """
        SELECT
            tpl.s_node_id, tpl.e_node_id,
            tpl.link_id, tpl.length, nl.cost
        FROM tb_path_links AS tpl
        JOIN nw_links       AS nl  ON tpl.link_id = nl.id
        WHERE tpl.path_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql_links, (path_id,))
        rows = cur.fetchall()

    # 2) Aggregate nodes, links, cost & length
    node_ids: Set[int] = {
        nid
        for row in rows
        for nid in (row["s_node_id"], row["e_node_id"])
        if nid is not None
    }
    link_ids   = {row["link_id"] for row in rows}
    total_length = sum(row["length"] or 0.0 for row in rows)
    total_cost   = sum(row["cost"]   or 0.0 for row in rows)

    # 3) Fetch node attributes in one IN‐clause
    nodes_data = []
    if node_ids:
        placeholders = ",".join("?" for _ in node_ids)
        sql_nodes = f"""
            SELECT id, e2e_group_no, data_code, utility_no, markers
            FROM nw_nodes
            WHERE id IN ({placeholders})
        """
        with conn.cursor() as cur:
            cur.execute(sql_nodes, tuple(node_ids))
            nodes_data = cur.fetchall()

    # 4) Collect distinct node attributes
    e2e_group_nos = {nd["e2e_group_no"] for nd in nodes_data if nd["e2e_group_no"] is not None}
    data_codes    = {nd["data_code"]     for nd in nodes_data}
    utility_nos   = {nd["utility_no"]    for nd in nodes_data if nd["utility_no"] is not None}

    # 5) Gather all markers into one string and extract
    all_markers = ",".join(nd["markers"] for nd in nodes_data if nd["markers"])
    references  = extract_markers(all_markers)

    return {
        "unique_nodes":     list(node_ids),
        "unique_links":     list(link_ids),
        "total_cost":       total_cost,
        "total_length":     total_length,
        "e2e_group_nos":    sorted(e2e_group_nos),
        "data_codes":       sorted(data_codes),
        "utility_nos":      sorted(utility_nos),
        "references":       references,
    }

