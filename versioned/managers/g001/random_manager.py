# managers/random.py

import random
from string_helper import StringHelper

class RandomSamplingManager:
    def __init__(self, db, run_cfg, bias_reduction=None):
        self.db = db
        self.run_cfg = run_cfg
        self.bias_reduction = bias_reduction or {}
        self._toolsets = None  # cache for toolsets

    def fetch_sampling_universe(self):
        # Get all toolset codes matching filters
        filters = {}
        rcfg = self.run_cfg.random_config
        if rcfg.fab_no:
            filters['fab_no'] = ('=', rcfg.fab_no)
        if rcfg.model_no:
            filters['model_no'] = ('=', rcfg.model_no)
        if rcfg.phase_no:
            filters['phase_no'] = ('=', rcfg.phase_no)
        if rcfg.toolset:
            filters['code'] = ('=', rcfg.toolset)
        filters['is_active'] = ('=', 1)

        where, params = StringHelper.build_where_clause(filters)
        sql = f'SELECT code FROM tb_toolsets{where}'
        rows = self.db.query(sql, params)
        self._toolsets = [r[0] for r in rows]
        return self._toolsets

    def fetch_random_toolset(self):
        # Pick a toolset code randomly
        if self._toolsets is None:
            self.fetch_sampling_universe()
        if not self._toolsets:
            return None
        return random.choice(self._toolsets)

    def fetch_equipment_ids(self, toolset_code):
        # Return all active equipment ids for the toolset
        sql = 'SELECT id FROM tb_equipments WHERE toolset = ? AND is_active = 1'
        rows = self.db.query(sql, [toolset_code])
        return [r[0] for r in rows]

    def fetch_pocs(self, equipment_id):
        # Return all PoC ids for this equipment (active)
        sql = 'SELECT id, node_id, is_used FROM tb_equipment_pocs WHERE equipment_id = ? AND is_active = 1'
        rows = self.db.query(sql, [equipment_id])
        return [{'poc_id': r[0], 'node_id': r[1], 'is_used': bool(r[2])} for r in rows]

    def fetch_random_poc_pair(self):
        # Full random: toolset -> two equipment -> one poc each (not same equipment)
        toolset_code = self.fetch_random_toolset()
        if not toolset_code:
            return None

        equipment_ids = self.fetch_equipment_ids(toolset_code)
        if len(equipment_ids) < 2:
            return None

        eq1, eq2 = random.sample(equipment_ids, 2)
        pocs1 = self.fetch_pocs(eq1)
        pocs2 = self.fetch_pocs(eq2)

        if not pocs1 or not pocs2:
            return None

        poc1 = random.choice(pocs1)
        poc2 = random.choice(pocs2)

        # Do not pick the same node or same PoC
        if poc1['node_id'] == poc2['node_id']:
            return None

        return (
            {
                'equipment_id': eq1,
                'poc_id': poc1['poc_id'],
                'node_id': poc1['node_id'],
                'is_used': poc1['is_used'],
            },
            {
                'equipment_id': eq2,
                'poc_id': poc2['poc_id'],
                'node_id': poc2['node_id'],
                'is_used': poc2['is_used'],
            }
        )

    def handle_missing_path(self, poc_pair, run_id):
        # If both PoCs are used, flag for manual review
        poc1, poc2 = poc_pair
        if poc1['is_used'] and poc2['is_used']:
            # Insert review flag into tb_review_flags
            sql = '''
                INSERT INTO tb_review_flags (
                    run_id, flag_type, severity, reason,
                    object_type, object_id, object_guid,
                    object_fab_no, object_model_no, object_data_code, object_e2e_group_no,
                    object_markers, object_utility_no, object_item_no, object_type_no,
                    object_is_loopback,
                    created_at, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
            '''
            # Only minimal fields used, fill the rest with None
            params = [
                run_id, 'MANUAL_REVIEW', 'MEDIUM', 'No traversable path between used PoCs',
                'POC', poc1['poc_id'], '',  # object_guid
                None, None, None, None, None, None, None, None, None,  # object meta
                '',  # notes
            ]
            self.db.update(sql, params)
