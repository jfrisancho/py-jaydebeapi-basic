# managers/validation.py

from datetime import datetime
from typing import Optional, list, dict, tuple

from db import Database
from string_helper import StringHelper

class ValidationManager:
    """Comprehensive path validation manager."""
    
    def __init__(self, db: Database):
        self.db = db
        self.validation_tests = self._load_validation_tests()
    
    def validate_run_paths(self, run_id: str) -> dict:
        """Validate all paths for a run and return summary results."""
        print(f'Starting validation for run {run_id}')
        
        # Get all paths for the run
        paths = self._fetch_run_paths_for_validation(run_id)
        
        total_paths = len(paths)
        validation_results = {
            'total_paths_validated': total_paths,
            'connectivity_errors': 0,
            'utility_errors': 0,
            'critical_errors': 0,
            'total_errors': 0,
            'total_flags': 0,
            'validation_time': 0
        }
        
        if not paths:
            print('No paths found for validation')
            return validation_results
        
        print(f'Validating {total_paths} paths')
        start_time = datetime.now()
        
        for i, path in enumerate(paths):
            try:
                # Validate connectivity
                connectivity_results = self._validate_path_connectivity(run_id, path)
                validation_results['connectivity_errors'] += connectivity_results['errors']
                
                # Validate utility consistency
                utility_results = self._validate_utility_consistency(run_id, path)
                validation_results['utility_errors'] += utility_results['errors']
                
                # Check for critical errors
                if connectivity_results['critical'] or utility_results['critical']:
                    validation_results['critical_errors'] += 1
                
                # Progress reporting
                if (i + 1) % 100 == 0:
                    print(f'Validated {i + 1}/{total_paths} paths')
                    
            except Exception as e:
                print(f'Error validating path {path["path_definition_id"]}: {e}')
                self._create_validation_error(
                    run_id, path['path_definition_id'], 'VALIDATION_SYSTEM', 'CRITICAL',
                    'SYSTEM', 0, '', 'Validation system error', str(e)
                )
                validation_results['critical_errors'] += 1
        
        # Calculate totals
        validation_results['total_errors'] = (
            validation_results['connectivity_errors'] + 
            validation_results['utility_errors']
        )
        
        # Get final flag count
        validation_results['total_flags'] = self._get_run_flag_count(run_id)
        
        validation_time = (datetime.now() - start_time).total_seconds()
        validation_results['validation_time'] = validation_time
        
        print(f'Validation completed in {validation_time:.1f}s: {validation_results["total_errors"]} errors, {validation_results["total_flags"]} flags')
        
        return validation_results
    
    def _validate_path_connectivity(self, run_id: str, path: dict) -> dict:
        """Validate path connectivity requirements."""
        results = {'errors': 0, 'critical': False}
        
        path_definition_id = path['path_definition_id']
        
        # Get path context to extract nodes and links
        path_context = path.get('path_context', '')
        if not path_context:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                'PATH', path_definition_id, '', 'Missing path context', 
                'Path context is empty or missing'
            )
            results['errors'] += 1
            results['critical'] = True
            return results
        
        # Parse path context
        from .path import PathManager
        path_manager = PathManager(self.db)
        path_data = path_manager.parse_path_context(path_context)
        
        nodes = path_data.get('nodes', [])
        links = path_data.get('links', [])
        
        if not nodes or not links:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                'PATH', path_definition_id, '', 'Empty path data',
                f'No nodes ({len(nodes)}) or links ({len(links)}) in path'
            )
            results['errors'] += 1
            results['critical'] = True
            return results
        
        # Validate start and end PoCs
        start_node_id = path['start_node_id']
        end_node_id = path['end_node_id']
        
        start_poc = self._get_poc_by_node_id(start_node_id)
        end_poc = self._get_poc_by_node_id(end_node_id)
        
        # Check start PoC connectivity requirements
        if start_poc:
            poc_errors = self._validate_poc_connectivity(run_id, path_definition_id, start_poc, 'START')
            results['errors'] += poc_errors
        else:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'CRITICAL',
                'NODE', start_node_id, '', 'Start PoC not found',
                f'No PoC found for start node {start_node_id}'
            )
            results['errors'] += 1
            results['critical'] = True
        
        # Check end PoC connectivity requirements
        if end_poc:
            poc_errors = self._validate_poc_connectivity(run_id, path_definition_id, end_poc, 'END')
            results['errors'] += poc_errors
        else:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'CRITICAL',
                'NODE', end_node_id, '', 'End PoC not found',
                f'No PoC found for end node {end_node_id}'
            )
            results['errors'] += 1
            results['critical'] = True
        
        # Validate path continuity
        continuity_errors = self._validate_path_continuity(run_id, path_definition_id, nodes, links)
        results['errors'] += continuity_errors
        
        return results
    
    def _validate_poc_connectivity(self, run_id: str, path_definition_id: int, poc: dict, position: str) -> int:
        """Validate PoC has required connectivity attributes."""
        errors = 0
        
        # Check required attributes for used PoCs
        if poc.get('is_used'):
            # Must have utility_no
            if not poc.get('utility_no'):
                self._create_validation_error(
                    run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                    'POC', poc['id'], poc.get('guid', ''), 'Missing utility number',
                    f'{position} PoC is marked as used but has no utility_no'
                )
                errors += 1
            
            # Must have markers
            if not poc.get('markers'):
                self._create_validation_error(
                    run_id, path_definition_id, 'CONNECTIVITY', 'MEDIUM',
                    'POC', poc['id'], poc.get('guid', ''), 'Missing markers',
                    f'{position} PoC is marked as used but has no markers'
                )
                errors += 1
            
            # Must have reference
            if not poc.get('reference'):
                self._create_validation_error(
                    run_id, path_definition_id, 'CONNECTIVITY', 'MEDIUM',
                    'POC', poc['id'], poc.get('guid', ''), 'Missing reference',
                    f'{position} PoC is marked as used but has no reference'
                )
                errors += 1
        
        # Check for loopback issues
        if poc.get('is_loopback') and position in ['START', 'END']:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'LOW',
                'POC', poc['id'], poc.get('guid', ''), 'Loopback PoC in path endpoint',
                f'{position} PoC is marked as loopback but used as path endpoint'
            )
            errors += 1
        
        return errors
    
    def _validate_path_continuity(self, run_id: str, path_definition_id: int, nodes: list[int], links: list[int]) -> int:
        """Validate that path nodes and links form a continuous path."""
        errors = 0
        
        if len(nodes) < 2:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'CRITICAL',
                'PATH', path_definition_id, '', 'Insufficient nodes',
                f'Path has only {len(nodes)} nodes, minimum 2 required'
            )
            return 1
        
        if len(links) != len(nodes) - 1:
            self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                'PATH', path_definition_id, '', 'Node-link count mismatch',
                f'Path has {len(nodes)} nodes but {len(links)} links (expected {len(nodes) - 1})'
            )
            errors += 1
        
        # Verify link connectivity
        for i, link_id in enumerate(links):
            link_info = self._get_link_info(link_id)
            if not link_info:
                self._create_validation_error(
                    run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                    'LINK', link_id, '', 'Link not found',
                    f'Link {link_id} at position {i} not found in database'
                )
                errors += 1
                continue
            
            # Check if link connects consecutive nodes
            if i < len(nodes) - 1:
                current_node = nodes[i]
                next_node = nodes[i + 1]
                
                link_connects = (
                    (link_info['start_node_id'] == current_node and link_info['end_node_id'] == next_node) or
                    (link_info['end_node_id'] == current_node and link_info['start_node_id'] == next_node and link_info['bidirected'] == 'Y')
                )
                
                if not link_connects:
                    self._create_validation_error(
                        run_id, path_definition_id, 'CONNECTIVITY', 'HIGH',
                        'LINK', link_id, '', 'Link connectivity mismatch',
                        f'Link {link_id} does not connect nodes {current_node} and {next_node}'
                    )
                    errors += 1
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path: dict) -> dict:
        """Validate utility consistency along the path."""
        results = {'errors': 0, 'critical': False}
        
        path_definition_id = path['path_definition_id']
        
        # Get path nodes
        path_context = path.get('path_context', '')
        if not path_context:
            return results
        
        from .path import PathManager
        path_manager = PathManager(self.db)
        path_data = path_manager.parse_path_context(path_context)
        nodes = path_data.get('nodes', [])
        
        if len(nodes) < 2:
            return results
        
        # Get utility information for all nodes in path
        node_utilities = self._get_node_utilities(nodes)
        
        # Check utility consistency
        start_node = nodes[0]
        end_node = nodes[-1]
        
        start_utility = node_utilities.get(start_node)
        end_utility = node_utilities.get(end_node)
        
        # Validate start and end utilities exist
        if not start_utility:
            self._create_validation_error(
                run_id, path_definition_id, 'UTILITY', 'MEDIUM',
                'NODE', start_node, '', 'Missing start utility',
                'Start node has no utility information'
            )
            results['errors'] += 1
        
        if not end_utility:
            self._create_validation_error(
                run_id, path_definition_id, 'UTILITY', 'MEDIUM',
                'NODE', end_node, '', 'Missing end utility',
                'End node has no utility information'
            )
            results['errors'] += 1
        
        # Check for utility transitions along path
        if start_utility and end_utility:
            utility_errors = self._validate_utility_transitions(
                run_id, path_definition_id, nodes, node_utilities
            )
            results['errors'] += utility_errors
        
        # Check for forbidden utility combinations
        forbidden_errors = self._check_forbidden_utility_combinations(
            run_id, path_definition_id, node_utilities
        )
        results['errors'] += forbidden_errors
        
        return results
    
    def _validate_utility_transitions(self, run_id: str, path_definition_id: int, 
                                    nodes: list[int], node_utilities: dict[int, dict]) -> int:
        """Validate utility transitions are allowed along the path."""
        errors = 0
        
        # Define allowed utility transitions
        allowed_transitions = {
            # Water utilities
            1: [1, 2, 3],  # Water can transition to water, steam, condensate
            2: [2, 1],     # Steam can transition to steam, water (condensation)
            3: [3, 1],     # Condensate can transition to condensate, water
            
            # Gas utilities
            10: [10, 11],  # N2 can transition to N2, compressed air
            11: [11, 10],  # Compressed air can transition to air, N2
            
            # Chemical utilities
            20: [20],      # CDA (Clean Dry Air) stays CDA
            21: [21],      # Special chemicals stay same
        }
        
        previous_utility = None
        for i, node_id in enumerate(nodes):
            utility_info = node_utilities.get(node_id)
            if not utility_info:
                continue
            
            current_utility = utility_info.get('utility_no')
            if current_utility is None:
                continue
            
            if previous_utility is not None and current_utility != previous_utility:
                # Check if transition is allowed
                allowed = allowed_transitions.get(previous_utility, [])
                if current_utility not in allowed:
                    self._create_validation_error(
                        run_id, path_definition_id, 'UTILITY', 'HIGH',
                        'NODE', node_id, '', 'Invalid utility transition',
                        f'Invalid transition from utility {previous_utility} to {current_utility} at node {node_id}'
                    )
                    errors += 1
            
            previous_utility = current_utility
        
        return errors
    
    def _check_forbidden_utility_combinations(self, run_id: str, path_definition_id: int,
                                            node_utilities: dict[int, dict]) -> int:
        """Check for forbidden utility combinations in the same path."""
        errors = 0
        
        utilities_in_path = set()
        for utility_info in node_utilities.values():
            utility_no = utility_info.get('utility_no')
            if utility_no:
                utilities_in_path.add(utility_no)
        
        # Define forbidden combinations
        forbidden_combinations = [
            {1, 20},   # Water and CDA should not mix
            {2, 10},   # Steam and N2 should not mix
            {3, 11},   # Condensate and compressed air should not mix
        ]
        
        for forbidden_set in forbidden_combinations:
            if forbidden_set.issubset(utilities_in_path):
                utilities_str = ', '.join(map(str, sorted(forbidden_set)))
                self._create_validation_error(
                    run_id, path_definition_id, 'UTILITY', 'CRITICAL',
                    'PATH', path_definition_id, '', 'Forbidden utility combination',
                    f'Path contains forbidden utility combination: {utilities_str}'
                )
                errors += 1
        
        return errors
    
    def _get_poc_by_node_id(self, node_id: int) -> Optional[dict]:
        """Get PoC information by node ID."""
        sql = '''
            SELECT 
                poc.id, poc.equipment_id, poc.node_id, poc.markers,
                poc.utility_no, poc.reference, poc.flow, poc.is_used,
                poc.is_loopback, eq.guid
            FROM tb_equipment_pocs poc
            JOIN tb_equipments eq ON poc.equipment_id = eq.id
            WHERE poc.node_id = ? AND poc.is_active = 1
        '''
        
        result = self.db.query(sql, [node_id])
        if result:
            row = result[0]
            return {
                'id': row[0],
                'equipment_id': row[1],
                'node_id': row[2],
                'markers': row[3],
                'utility_no': row[4],
                'reference': row[5],
                'flow': row[6],
                'is_used': bool(row[7]),
                'is_loopback': bool(row[8]),
                'guid': row[9]
            }
        return None
    
    def _get_link_info(self, link_id: int) -> Optional[dict]:
        """Get link information by ID."""
        sql = '''
            SELECT start_node_id, end_node_id, bidirected, cost
            FROM nw_links
            WHERE id = ?
        '''
        
        result = self.db.query(sql, [link_id])
        if result:
            row = result[0]
            return {
                'start_node_id': row[0],
                'end_node_id': row[1],
                'bidirected': row[2],
                'cost': row[3]
            }
        return None
    
    def _get_node_utilities(self, node_ids: list[int]) -> dict[int, dict]:
        """Get utility information for multiple nodes."""
        if not node_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(node_ids))
        sql = f'''
            SELECT id, utility_no, markers, data_code
            FROM nw_nodes
            WHERE id IN ({placeholders})
        '''
        
        results = self.db.query(sql, node_ids)
        
        utilities = {}
        for row in results:
            utilities[row[0]] = {
                'utility_no': row[1],
                'markers': row[2],
                'data_code': row[3]
            }
        
        return utilities
    
    def _fetch_run_paths_for_validation(self, run_id: str) -> list[dict]:
        """Fetch paths for validation with necessary information."""
        sql = '''
            SELECT 
                ap.path_definition_id,
                ap.start_node_id,
                ap.end_node_id,
                ap.cost,
                pd.path_context,
                pd.node_count,
                pd.link_count
            FROM tb_attempt_paths ap
            JOIN tb_path_definitions pd ON ap.path_definition_id = pd.id
            WHERE ap.run_id = ?
        '''
        
        results = self.db.query(sql, [run_id])
        
        paths = []
        for row in results:
            paths.append({
                'path_definition_id': row[0],
                'start_node_id': row[1],
                'end_node_id': row[2],
                'cost': row[3],
                'path_context': row[4],
                'node_count': row[5],
                'link_count': row[6]
            })
        
        return paths
    
    def _create_validation_error(self, run_id: str, path_definition_id: Optional[int],
                               error_scope: str, severity: str, object_type: str,
                               object_id: int, object_guid: str, error_type: str,
                               error_message: str, error_data: str = ''):
        """Create a validation error record."""
        sql = '''
            INSERT INTO tb_validation_errors (
                run_id, path_definition_id, severity, error_scope, error_type,
                object_type, object_id, object_guid, error_message, error_data,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            path_definition_id,
            severity,
            error_scope,
            error_type,
            object_type,
            object_id,
            object_guid,
            error_message,
            error_data,
            StringHelper.datetime_to_sqltimestamp(datetime.now())
        ]
        
        self.db.update(sql, params)
    
    def _get_run_flag_count(self, run_id: str) -> int:
        """Get total count of review flags for a run."""
        sql = 'SELECT COUNT(*) FROM tb_review_flags WHERE run_id = ?'
        result = self.db.query(sql, [run_id])
        return result[0][0] if result else 0
    
    def _load_validation_tests(self) -> dict:
        """Load validation test configurations from database."""
        sql = '''
            SELECT code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests
            WHERE is_active = 1
        '''
        
        results = self.db.query(sql)
        
        tests = {}
        for row in results:
            tests[row[0]] = {
                'name': row[1],
                'scope': row[2],
                'severity': row[3],
                'test_type': row[4],
                'is_active': bool(row[5]),
                'description': row[6]
            }
        
        return tests
    
    def fetch_validation_summary(self, run_id: str) -> dict:
        """Fetch validation summary for a run."""
        sql = '''
            SELECT 
                severity,
                error_scope,
                COUNT(*) as error_count
            FROM tb_validation_errors
            WHERE run_id = ?
            GROUP BY severity, error_scope
            ORDER BY severity, error_scope
        '''
        
        results = self.db.query(sql, [run_id])
        
        summary = {
            'total_errors': 0,
            'by_severity': {},
            'by_scope': {},
            'critical_count': 0,
            'high_count': 0,
            'medium_count': 0,
            'low_count': 0
        }
        
        for row in results:
            severity, scope, count = row
            summary['total_errors'] += count
            
            if severity not in summary['by_severity']:
                summary['by_severity'][severity] = 0
            summary['by_severity'][severity] += count
            
            if scope not in summary['by_scope']:
                summary['by_scope'][scope] = 0
            summary['by_scope'][scope] += count
            
            # Count by severity
            if severity == 'CRITICAL':
                summary['critical_count'] += count
            elif severity == 'HIGH':
                summary['high_count'] += count
            elif severity == 'MEDIUM':
                summary['medium_count'] += count
            elif severity == 'LOW':
                summary['low_count'] += count
        
        return summary
    
    def fetch_validation_errors(self, run_id: str, severity: Optional[str] = None,
                              error_scope: Optional[str] = None) -> list[dict]:
        """Fetch validation errors with optional filtering."""
        filters = {'run_id': ('=', run_id)}
        
        if severity:
            filters['severity'] = ('=', severity)
        if error_scope:
            filters['error_scope'] = ('=', error_scope)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT 
                id, path_definition_id, severity, error_scope, error_type,
                object_type, object_id, object_guid, error_message,
                error_data, created_at
            FROM tb_validation_errors
            {where_clause}
            ORDER BY created_at DESC
        '''
        
        results = self.db.query(sql, params)
        
        errors = []
        for row in results:
            errors.append({
                'id': row[0],
                'path_definition_id': row[1],
                'severity': row[2],
                'error_scope': row[3],
                'error_type': row[4],
                'object_type': row[5],
                'object_id': row[6],
                'object_guid': row[7],
                'error_message': row[8],
                'error_data': row[9],
                'created_at': row[10]
            })
        
        return errors