# managers/coverage.py

from bitarray import bitarray

class CoverageManager:
    def __init__(self, db, run_cfg):
        self.db = db
        self.run_cfg = run_cfg
        self.node_id_mapping = {}   # {node_id: idx}
        self.link_id_mapping = {}   # {link_id: idx}
        self.total_nodes = 0
        self.total_links = 0
        self._init_scope()

        self.covered_nodes = bitarray(self.total_nodes)
        self.covered_nodes.setall(0)
        self.covered_links = bitarray(self.total_links)
        self.covered_links.setall(0)
        self._covered_node_count = 0
        self._covered_link_count = 0

    def _init_scope(self):
        # Scope is defined by fab/model/phase/toolset filters in run_cfg
        filters = {}
        rcfg = self.run_cfg.random_config
        if rcfg.fab_no:
            filters['fab_no'] = ('=', rcfg.fab_no)
        if rcfg.model_no:
            filters['model_no'] = ('=', rcfg.model_no)
        if rcfg.phase_no:
            filters['phase_no'] = ('=', rcfg.phase_no)
        if rcfg.toolset:
            filters['toolset'] = ('=', rcfg.toolset)
        filters['is_active'] = ('=', 1)

        # Nodes in scope
        where, params = self._build_node_where_clause(filters)
        sql_nodes = f'SELECT id FROM nw_nodes{where}'
        nodes = [r[0] for r in self.db.query(sql_nodes, params)]
        self.total_nodes = len(nodes)
        self.node_id_mapping = {nid: i for i, nid in enumerate(nodes)}

        # Links in scope: Only links where both nodes are in scope
        sql_links = f'''
            SELECT id FROM nw_links
            WHERE start_node_id IN ({",".join(str(n) for n in nodes)})
              AND end_node_id IN ({",".join(str(n) for n in nodes)})
        '''
        links = [r[0] for r in self.db.query(sql_links)]
        self.total_links = len(links)
        self.link_id_mapping = {lid: i for i, lid in enumerate(links)}

    def _build_node_where_clause(self, filters):
        # Only the columns that exist in nw_nodes
        valid = {}
        for k in ('fab_no', 'model_no', 'phase_no'):
            if k in filters:
                valid[k] = filters[k]
        return self.run_cfg.StringHelper.build_where_clause(valid)

    def update_coverage(self, path):
        # Add new nodes/links to bitarrays
        new_nodes = 0
        new_links = 0
        for nid in path.nodes:
            idx = self.node_id_mapping.get(nid)
            if idx is not None and not self.covered_nodes[idx]:
                self.covered_nodes[idx] = 1
                new_nodes += 1
        for lid in path.links:
            idx = self.link_id_mapping.get(lid)
            if idx is not None and not self.covered_links[idx]:
                self.covered_links[idx] = 1
                new_links += 1
        if new_nodes or new_links:
            self._covered_node_count += new_nodes
            self._covered_link_count += new_links
            return True  # Some coverage increased
        return False

    def current_coverage_pct(self):
        num = self._covered_node_count + self._covered_link_count
        denom = (self.total_nodes + self.total_links) or 1
        return float(num) / denom * 100

    def has_met_coverage_target(self):
        if not self.run_cfg.random_config.coverage_target:
            return False
        return self.current_coverage_pct() >= (self.run_cfg.random_config.coverage_target * 100)

    def store_coverage_summary(self, run_id):
        sql = '''
            INSERT INTO tb_run_coverage_summary (
                run_id, total_nodes_in_scope, total_links_in_scope,
                covered_nodes, covered_links,
                node_coverage_pct, link_coverage_pct, overall_coverage_pct,
                unique_paths_count, scope_filters, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        '''
        node_pct = float(self._covered_node_count) / (self.total_nodes or 1) * 100
        link_pct = float(self._covered_link_count) / (self.total_links or 1) * 100
        overall_pct = self.current_coverage_pct()
        filters = str({
            'fab_no': self.run_cfg.random_config.fab_no,
            'model_no': self.run_cfg.random_config.model_no,
            'phase_no': self.run_cfg.random_config.phase_no,
            'toolset': self.run_cfg.random_config.toolset
        })
        params = [
            run_id, self.total_nodes, self.total_links,
            self._covered_node_count, self._covered_link_count,
            node_pct, link_pct, overall_pct,
            0,  # unique_paths_count (could be filled in from path manager)
            filters
        ]
        self.db.update(sql, params)
