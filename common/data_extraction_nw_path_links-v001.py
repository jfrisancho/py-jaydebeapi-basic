import re
from typing import Any  # still needed for conn: Any, but you can drop it if you like

def extract_markers(markers: str, only_label: bool = False) -> list[str]:
    pattern = r'([A-Za-z]+\d+)(\(\d+\))?'
    matches = re.findall(pattern, markers)
    return [p for p, _ in matches] if only_label else [p + s for p, s in matches]

def get_path_info(conn: Any, path_id: int) -> dict[str, Any]:
    """
    Returns a dict with:
      - unique_nodes: list[int]
      - unique_links: list[int]
      - total_cost: float
      - total_length: float
      - e2e_group_nos: list[int]
      - data_codes: list[int]
      - utility_nos: list[int]
      - references: list[str]
    """
    sql_links = """
        SELECT tpl.s_node_id, tpl.e_node_id,
               tpl.link_id, tpl.length, nl.cost
          FROM tb_path_links tpl
          JOIN nw_links       nl  ON tpl.link_id = nl.id
         WHERE tpl.path_id = ?
    """
    with conn.cursor() as cur:
        cur.execute(sql_links, (path_id,))
        rows = cur.fetchall()

    # build sets of nodes & links
    node_ids = {n for row in rows for n in (row["s_node_id"], row["e_node_id"]) if n is not None}
    link_ids = {row["link_id"] for row in rows}

    total_length = sum((row["length"] or 0.0) for row in rows)
    total_cost   = sum((row["cost"]   or 0.0) for row in rows)

    # fetch node attrs in one go
    nodes_data: list[dict[str, Any]] = []
    if node_ids:
        ph = ",".join("?" for _ in node_ids)
        sql_nodes = f"""
            SELECT id, e2e_group_no, data_code, utility_no, markers
              FROM nw_nodes
             WHERE id IN ({ph})
        """
        with conn.cursor() as cur:
            cur.execute(sql_nodes, tuple(node_ids))
            nodes_data = cur.fetchall()

    e2e_group_nos = {nd["e2e_group_no"] for nd in nodes_data if nd["e2e_group_no"] is not None}
    data_codes    = {nd["data_code"]     for nd in nodes_data}
    utility_nos   = {nd["utility_no"]    for nd in nodes_data if nd["utility_no"] is not None}

    all_markers = ",".join(nd["markers"] for nd in nodes_data if nd["markers"])
    references  = extract_markers(all_markers)

    return {
        "unique_nodes":  list(node_ids),
        "unique_links":  list(link_ids),
        "total_cost":    total_cost,
        "total_length":  total_length,
        "e2e_group_nos": sorted(e2e_group_nos),
        "data_codes":    sorted(data_codes),
        "utility_nos":   sorted(utility_nos),
        "references":    references,
    }
