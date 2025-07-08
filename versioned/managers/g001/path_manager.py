# managers/path.py

from string_helper import StringHelper
from models import PathResult
from collections import deque

class PathManager:
    def __init__(self, db, run_cfg):
        self.db = db
        self.run_cfg = run_cfg
        self.graph = None  # lazy loaded as {node_id: [neighbor_node_id, ...]}

    def _load_graph(self, toolset_code):
        # Loads a graph of nodes and their neighbors for the toolset (unidirectional)
        sql = '''
            SELECT n.id, l.start_node_id, l.end_node_id, l.id
            FROM nw_nodes n
            JOIN nw_links l ON l.start_node_id = n.id
            WHERE n.id IN (
                SELECT node_id FROM tb_equipments WHERE toolset = ?
                UNION
                SELECT node_id FROM tb_equipment_pocs ep JOIN tb_equipments e ON ep.equipment_id = e.id WHERE e.toolset = ?
            )
        '''
        rows = self.db.query(sql, [toolset_code, toolset_code])
        graph = {}
        for row in rows:
            node_id, start_id, end_id, link_id = row
            if start_id not in graph:
                graph[start_id] = []
            graph[start_id].append((end_id, link_id))
        self.graph = graph

    def fetch_path_between_pocs(self, poc1, poc2):
        # Build the graph for the toolset if needed
        toolset_code = self._get_toolset_code(poc1, poc2)
        if self.graph is None:
            self._load_graph(toolset_code)

        start = poc1['node_id']
        end = poc2['node_id']
        if start == end:
            return None

        # BFS pathfinding
        queue = deque([(start, [], [])])  # (current_node, nodes_seq, links_seq)
        visited = set()
        found = False
        path_nodes = []
        path_links = []
        while queue:
            current, nodes, links = queue.popleft()
            if current in visited:
                continue
            visited.add(current)
            nodes = nodes + [current]
            if current == end:
                path_nodes = nodes
                path_links = links
                found = True
                break
            for neighbor, link_id in self.graph.get(current, []):
                if neighbor not in visited:
                    queue.append((neighbor, nodes, links + [link_id]))

        if not found or not path_nodes or not path_links:
            return None

        # Gather info for PathResult
        path_hash = StringHelper.generate_path_hash(path_nodes, path_links)
        path = PathResult(
            start_node_id=poc1['node_id'],
            start_poc_id=poc1['poc_id'],
            start_equipment_id=poc1['equipment_id'],
            end_node_id=poc2['node_id'],
            end_poc_id=poc2['poc_id'],
            end_equipment_id=poc2['equipment_id'],
            nodes=path_nodes,
            links=path_links,
            total_cost=0.0,      # Optionally sum link costs
            total_length_mm=0.0, # Optionally sum link lengths
            toolset_nos=[],      # Could be filled in
            data_codes=[],
            utility_nos=[],
            references=[],
        )
        path.hash = path_hash
        return path

    def store_path_result(self, path, run_id):
        # Only insert if unique by hash
        sql_check = 'SELECT id FROM tb_path_definitions WHERE path_hash = ?'
        rows = self.db.query(sql_check, [path.hash])
        if rows:
            path_id = rows[0][0]
        else:
            # Insert path definition
            sql_insert = '''
                INSERT INTO tb_path_definitions (
                    path_hash, source_type, scope, 
                    target_fab_no, target_model_no, target_phase_no, target_toolset_no,
                    node_count, link_count, total_length_mm, coverage,
                    path_context, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            '''
            params = [
                path.hash, 'RANDOM', 'CONNECTIVITY',
                None, None, None, None,
                len(path.nodes), len(path.links), path.total_length_mm, 0.0,
                str({'nodes': path.nodes, 'links': path.links}),
            ]
            self.db.update(sql_insert, params)
            path_id = self.db.query('SELECT id FROM tb_path_definitions WHERE path_hash = ?', [path.hash])[0][0]

        # Insert attempt (sampling attempt record)
        sql_attempt = '''
            INSERT INTO tb_attempt_paths (
                run_id, path_definition_id,
                start_node_id, end_node_id, cost,
                picked_at
            ) VALUES (?, ?, ?, ?, ?, now())
        '''
        params = [run_id, path_id, path.start_node_id, path.end_node_id, path.total_cost]
        self.db.update(sql_attempt, params)

    def _get_toolset_code(self, poc1, poc2):
        # Get the toolset code for the two PoCs (assuming both from same toolset)
        # You may want to make this more robust
        sql = 'SELECT e.toolset FROM tb_equipments e JOIN tb_equipment_pocs ep ON ep.equipment_id = e.id WHERE ep.id = ?'
        toolset1 = self.db.query(sql, [poc1['poc_id']])[0][0]
        toolset2 = self.db.query(sql, [poc2['poc_id']])[0][0]
        if toolset1 != toolset2:
            raise Exception('PoCs not from same toolset, inter-toolset search not supported')
        return toolset1
