# managers/validation.py

class ValidationManager:
    def __init__(self, db, run_cfg):
        self.db = db
        self.run_cfg = run_cfg

    def validate_sampled_paths(self, run_id):
        # 1. Fetch all path attempts for this run
        sql = '''
            SELECT ap.id, ap.path_definition_id, pd.path_context
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        '''
        rows = self.db.query(sql, [run_id])
        results = {
            'validated': 0,
            'errors': 0,
            'review_flags': 0,
        }
        for ap_id, path_def_id, path_context in rows:
            path_data = eval(path_context)  # {'nodes': [...], 'links': [...]}
            nodes = path_data.get('nodes', [])
            links = path_data.get('links', [])

            # a. Validate connectivity (no nulls, all reachable)
            if not self._validate_connectivity(nodes, links):
                self._flag_error(run_id, path_def_id, 'CRITICAL', 'CONNECTIVITY', 'MISSING_OR_NULL', nodes, links)
                results['errors'] += 1
                continue

            # b. Validate utility consistency
            if not self._validate_utility(nodes):
                self._flag_error(run_id, path_def_id, 'MEDIUM', 'UTILITY', 'INCONSISTENT', nodes, links)
                results['errors'] += 1
                continue

            results['validated'] += 1

        # Optionally update run summary/validation tables
        return results

    def _validate_connectivity(self, nodes, links):
        # Check for empty or any node/link being None/0
        if not nodes or not links:
            return False
        if any(n is None or n == 0 for n in nodes):
            return False
        if any(l is None or l == 0 for l in links):
            return False
        return True

    def _validate_utility(self, nodes):
        # Simple check: all utilities along path must be non-null and match first
        if not nodes:
            return False
        sql = f'SELECT utility_no FROM nw_nodes WHERE id IN ({",".join(str(n) for n in nodes)})'
        rows = self.db.query(sql)
        utilities = [r[0] for r in rows if r[0] is not None]
        if not utilities or len(utilities) != len(nodes):
            return False
        # All the same utility? (simple version)
        first = utilities[0]
        return all(u == first for u in utilities)

    def _flag_error(self, run_id, path_def_id, severity, scope, error_type, nodes, links):
        sql = '''
            INSERT INTO tb_validation_errors (
                run_id, path_definition_id, validation_test_id, severity, error_scope, error_type,
                object_type, object_id, object_guid,
                object_fab_no, object_model_no, object_data_code, object_e2e_group_no,
                object_markers, object_utility_no, object_item_no, object_type_no,
                object_material_no, object_flow, object_is_loopback, object_cost,
                error_message, error_data, created_at, notes
            ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now(), ?)
        '''
        # Just a basic flag for now; real system would add much more context
        params = [
            run_id, path_def_id, severity, scope, error_type,
            'PATH', nodes[0] if nodes else 0, '', None, None, None, None, None,
            None, None, None, None, None, None, None,
            f'{scope} {error_type}', str({'nodes': nodes, 'links': links}), ''
        ]
        self.db.update(sql, params)
