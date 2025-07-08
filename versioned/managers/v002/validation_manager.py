# managers/validation.py

from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime
from enum import Enum

from db import Database
from string_helper import StringHelper


class ValidationSeverity(Enum):
    """Validation severity levels."""
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class ValidationScope(Enum):
    """Validation scope types."""
    CONNECTIVITY = 'CONNECTIVITY'
    FLOW = 'FLOW'
    MATERIAL = 'MATERIAL'
    QA = 'QA'
    STRUCTURAL = 'STRUCTURAL'


class ValidationManager:
    """Comprehensive path validation framework."""
    
    def __init__(self, db: Database):
        self.db = db
        self.validation_tests = {}
        self._load_validation_tests()

    def validate_run_paths(self, run_id: str) -> Dict[str, Any]:
        """Validate all paths in a run and return comprehensive results."""
        
        print(f'Starting validation for run {run_id}')
        
        # Get all unique paths for the run
        paths = self._fetch_run_paths(run_id)
        
        validation_results = {
            'total_paths_validated': len(paths),
            'total_errors': 0,
            'total_review_flags': 0,
            'critical_errors': 0,
            'errors_by_severity': {},
            'errors_by_type': {},
            'validation_summary': {}
        }
        
        for path in paths:
            # Validate connectivity
            connectivity_errors = self._validate_connectivity(run_id, path)
            
            # Validate utility consistency
            utility_errors = self._validate_utility_consistency(run_id, path)
            
            # Validate structural integrity
            structural_errors = self._validate_structural_integrity(run_id, path)
            
            # Process all errors
            all_errors = connectivity_errors + utility_errors + structural_errors
            
            for error in all_errors:
                self._store_validation_error(run_id, path['path_definition_id'], error)
                self._update_validation_results(validation_results, error)
                
                # Create review flags for critical issues
                if error['severity'] in ['HIGH', 'CRITICAL']:
                    self._create_review_flag(run_id, path, error)
                    validation_results['total_review_flags'] += 1
        
        # Generate validation summary
        validation_results['validation_summary'] = self._generate_validation_summary(run_id)
        
        print(f'Validation completed: {validation_results["total_errors"]} errors found')
        
        return validation_results

    def _validate_connectivity(self, run_id: str, path: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate path connectivity and completeness."""
        errors = []
        path_context = self._parse_path_context(path.get('path_context', ''))
        
        # Test CONN_001: Verify all nodes exist and are accessible
        missing_nodes = self._check_missing_nodes(path_context.get('nodes', []))
        if missing_nodes:
            errors.append({
                'test_code': 'CONN_001',
                'severity': ValidationSeverity.CRITICAL.value,
                'error_type': 'MISSING_NODES',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'NODE',
                'object_ids': missing_nodes,
                'error_message': f'Missing or inaccessible nodes: {missing_nodes}',
                'error_data': f'path_nodes_count={len(path_context.get("nodes", []))}'
            })
        
        # Test CONN_002: Verify all links exist and are traversable
        missing_links = self._check_missing_links(path_context.get('links', []))
        if missing_links:
            errors.append({
                'test_code': 'CONN_002',
                'severity': ValidationSeverity.CRITICAL.value,
                'error_type': 'MISSING_LINKS',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'LINK',
                'object_ids': missing_links,
                'error_message': f'Missing or non-traversable links: {missing_links}',
                'error_data': f'path_links_count={len(path_context.get("links", []))}'
            })
        
        # Test CONN_003: Verify path continuity
        continuity_errors = self._check_path_continuity(path_context.get('nodes', []), 
                                                       path_context.get('links', []))
        errors.extend(continuity_errors)
        
        # Test CONN_004: Verify PoC connectivity
        poc_errors = self._validate_poc_connectivity(path)
        errors.extend(poc_errors)
        
        return errors

    def _validate_utility_consistency(self, run_id: str, path: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate utility consistency along the path."""
        errors = []
        path_context = self._parse_path_context(path.get('path_context', ''))
        
        # Get utility information for all nodes in path
        utility_sequence = self._get_path_utility_sequence(path_context.get('nodes', []))
        
        # Test UTIL_001: Check for utility null values where they shouldn't be
        null_utility_errors = self._check_null_utilities(utility_sequence)
        errors.extend(null_utility_errors)
        
        # Test UTIL_002: Validate utility transitions
        transition_errors = self._validate_utility_transitions(utility_sequence)
        errors.extend(transition_errors)
        
        # Test UTIL_003: Check for illegal utility combinations
        combination_errors = self._check_utility_combinations(utility_sequence)
        errors.extend(combination_errors)
        
        # Test UTIL_004: Validate utility flow direction consistency
        flow_errors = self._validate_utility_flow_direction(path, utility_sequence)
        errors.extend(flow_errors)
        
        return errors

    def _validate_structural_integrity(self, run_id: str, path: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate structural integrity of the path."""
        errors = []
        path_context = self._parse_path_context(path.get('path_context', ''))
        
        # Test STRUCT_001: Check for isolated nodes
        isolated_nodes = self._check_isolated_nodes(path_context.get('nodes', []))
        if isolated_nodes:
            errors.append({
                'test_code': 'STRUCT_001',
                'severity': ValidationSeverity.MEDIUM.value,
                'error_type': 'ISOLATED_NODES',
                'error_scope': 'STRUCTURAL',
                'object_type': 'NODE',
                'object_ids': isolated_nodes,
                'error_message': f'Isolated nodes found in path: {isolated_nodes}',
                'error_data': f'isolated_count={len(isolated_nodes)}'
            })
        
        # Test STRUCT_002: Validate path length reasonableness
        length_errors = self._validate_path_length(path)
        errors.extend(length_errors)
        
        # Test STRUCT_003: Check for circular references
        circular_errors = self._check_circular_references(path_context.get('nodes', []))
        errors.extend(circular_errors)
        
        return errors

    def _check_missing_nodes(self, node_ids: List[int]) -> List[int]:
        """Check for missing or inaccessible nodes."""
        if not node_ids:
            return []
        
        placeholders = ','.join(['?' for _ in node_ids])
        sql = f'SELECT id FROM nw_nodes WHERE id IN ({placeholders})'
        
        existing_rows = self.db.query(sql, node_ids)
        existing_nodes = set(row[0] for row in existing_rows)
        
        return [node_id for node_id in node_ids if node_id not in existing_nodes]

    def _check_missing_links(self, link_ids: List[int]) -> List[int]:
        """Check for missing or non-traversable links."""
        if not link_ids:
            return []
        
        placeholders = ','.join(['?' for _ in link_ids])
        sql = f'SELECT id FROM nw_links WHERE id IN ({placeholders})'
        
        existing_rows = self.db.query(sql, link_ids)
        existing_links = set(row[0] for row in existing_rows)
        
        return [link_id for link_id in link_ids if link_id not in existing_links]

    def _check_path_continuity(self, nodes: List[int], links: List[int]) -> List[Dict[str, Any]]:
        """Check if path forms a continuous sequence."""
        errors = []
        
        if len(nodes) < 2 or len(links) < 1:
            return errors
        
        # Get link connectivity information
        placeholders = ','.join(['?' for _ in links])
        sql = f'''
        SELECT id, start_node_id, end_node_id, bidirected
        FROM nw_links 
        WHERE id IN ({placeholders})
        ORDER BY id
        '''
        
        link_rows = self.db.query(sql, links)
        link_connections = {row[0]: {'start': row[1], 'end': row[2], 'bidirected': row[3]} 
                          for row in link_rows}
        
        # Verify each link connects consecutive nodes
        for i in range(len(links)):
            link_id = links[i]
            if link_id not in link_connections:
                continue
            
            link_info = link_connections[link_id]
            current_node = nodes[i] if i < len(nodes) else None
            next_node = nodes[i + 1] if i + 1 < len(nodes) else None
            
            if current_node and next_node:
                # Check if link connects these nodes
                connects = (
                    (link_info['start'] == current_node and link_info['end'] == next_node) or
                    (link_info['bidirected'] == 'Y' and 
                     link_info['start'] == next_node and link_info['end'] == current_node)
                )
                
                if not connects:
                    errors.append({
                        'test_code': 'CONN_003',
                        'severity': ValidationSeverity.HIGH.value,
                        'error_type': 'PATH_DISCONTINUITY',
                        'error_scope': 'CONNECTIVITY',
                        'object_type': 'LINK',
                        'object_ids': [link_id],
                        'error_message': f'Link {link_id} does not connect nodes {current_node} and {next_node}',
                        'error_data': f'expected_connection={current_node}->{next_node};actual_connection={link_info["start"]}->{link_info["end"]}'
                    })
        
        return errors

    def _validate_poc_connectivity(self, path: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate that PoCs are properly connected and have required attributes."""
        errors = []
        
        # Get start and end PoC information from attempt paths
        sql = '''
        SELECT ap.start_node_id, ap.end_node_id
        FROM tb_attempt_paths ap
        WHERE ap.path_definition_id = ?
        LIMIT 1
        '''
        
        attempt_rows = self.db.query(sql, [path['path_definition_id']])
        if not attempt_rows:
            return errors
        
        start_node_id, end_node_id = attempt_rows[0]
        
        # Validate start PoC
        start_poc_errors = self._validate_single_poc(start_node_id, 'START')
        errors.extend(start_poc_errors)
        
        # Validate end PoC
        end_poc_errors = self._validate_single_poc(end_node_id, 'END')
        errors.extend(end_poc_errors)
        
        return errors

    def _validate_single_poc(self, node_id: int, poc_type: str) -> List[Dict[str, Any]]:
        """Validate a single PoC for required attributes."""
        errors = []
        
        # Get PoC information
        sql = '''
        SELECT p.id, p.utility_no, p.markers, p.reference, p.is_used
        FROM tb_equipment_pocs p
        WHERE p.node_id = ? AND p.is_active = 1
        '''
        
        poc_rows = self.db.query(sql, [node_id])
        if not poc_rows:
            errors.append({
                'test_code': 'CONN_004',
                'severity': ValidationSeverity.CRITICAL.value,
                'error_type': 'MISSING_POC',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'POC',
                'object_ids': [node_id],
                'error_message': f'{poc_type} PoC not found for node {node_id}',
                'error_data': f'poc_type={poc_type};node_id={node_id}'
            })
            return errors
        
        poc_id, utility_no, markers, reference, is_used = poc_rows[0]
        
        # Check required attributes
        if not utility_no:
            errors.append({
                'test_code': 'CONN_005',
                'severity': ValidationSeverity.HIGH.value,
                'error_type': 'NULL_UTILITY',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'POC',
                'object_ids': [poc_id],
                'error_message': f'{poc_type} PoC {poc_id} has null utility_no',
                'error_data': f'poc_type={poc_type};poc_id={poc_id}'
            })
        
        if not markers:
            errors.append({
                'test_code': 'CONN_006',
                'severity': ValidationSeverity.MEDIUM.value,
                'error_type': 'NULL_MARKERS',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'POC',
                'object_ids': [poc_id],
                'error_message': f'{poc_type} PoC {poc_id} has null markers',
                'error_data': f'poc_type={poc_type};poc_id={poc_id}'
            })
        
        if not reference:
            errors.append({
                'test_code': 'CONN_007',
                'severity': ValidationSeverity.MEDIUM.value,
                'error_type': 'NULL_REFERENCE',
                'error_scope': 'CONNECTIVITY',
                'object_type': 'POC',
                'object_ids': [poc_id],
                'error_message': f'{poc_type} PoC {poc_id} has null reference',
                'error_data': f'poc_type={poc_type};poc_id={poc_id}'
            })
        
        return errors

    def _get_path_utility_sequence(self, node_ids: List[int]) -> List[Dict[str, Any]]:
        """Get utility information sequence for nodes in path."""
        if not node_ids:
            return []
        
        placeholders = ','.join(['?' for _ in node_ids])
        sql = f'''
        SELECT n.id, n.utility_no, n.markers, n.data_code
        FROM nw_nodes n
        WHERE n.id IN ({placeholders})
        ORDER BY CASE {' '.join([f'WHEN n.id = ? THEN {i}' for i, _ in enumerate(node_ids)])} END
        '''
        
        # Flatten node_ids for the ORDER BY CASE parameters
        params = node_ids + node_ids
        
        rows = self.db.query(sql, params)
        
        return [
            {
                'node_id': row[0],
                'utility_no': row[1],
                'markers': row[2],
                'data_code': row[3]
            }
            for row in rows
        ]

    def _check_null_utilities(self, utility_sequence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check for null utilities where they shouldn't be."""
        errors = []
        
        for node_info in utility_sequence:
            if node_info['utility_no'] is None:
                errors.append({
                    'test_code': 'UTIL_001',
                    'severity': ValidationSeverity.MEDIUM.value,
                    'error_type': 'NULL_UTILITY',
                    'error_scope': 'UTILITY',
                    'object_type': 'NODE',
                    'object_ids': [node_info['node_id']],
                    'error_message': f'Node {node_info["node_id"]} has null utility',
                    'error_data': f'data_code={node_info["data_code"]};markers={node_info["markers"]}'
                })
        
        return errors

    def _validate_utility_transitions(self, utility_sequence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate utility transitions along the path."""
        errors = []
        
        # Define valid utility transitions (simplified example)
        valid_transitions = {
            # Water utilities
            1: [1, 2, 3],  # Water can transition to hot water, steam
            2: [1, 2, 3],  # Hot water can transition to water, steam
            3: [1, 2, 3],  # Steam can condense to water
            # Gas utilities
            10: [10, 11],  # N2 can transition to compressed N2
            11: [10, 11],  # Compressed N2 can transition to N2
            # Add more utility transition rules as needed
        }
        
        for i in range(len(utility_sequence) - 1):
            current_utility = utility_sequence[i]['utility_no']
            next_utility = utility_sequence[i + 1]['utility_no']
            
            if current_utility and next_utility:
                allowed_transitions = valid_transitions.get(current_utility, [current_utility])
                
                if next_utility not in allowed_transitions:
                    errors.append({
                        'test_code': 'UTIL_002',
                        'severity': ValidationSeverity.HIGH.value,
                        'error_type': 'INVALID_UTILITY_TRANSITION',
                        'error_scope': 'UTILITY',
                        'object_type': 'NODE',
                        'object_ids': [utility_sequence[i]['node_id'], utility_sequence[i + 1]['node_id']],
                        'error_message': f'Invalid utility transition from {current_utility} to {next_utility}',
                        'error_data': f'transition={current_utility}->{next_utility};position={i}'
                    })
        
        return errors

    def _check_utility_combinations(self, utility_sequence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Check for illegal utility combinations in the same path."""
        errors = []
        
        # Define incompatible utility pairs
        incompatible_utilities = {
            (1, 10),   # Water and N2 should not mix
            (2, 15),   # Hot water and compressed air incompatible
            # Add more incompatible combinations
        }
        
        utilities_in_path = set(node['utility_no'] for node in utility_sequence if node['utility_no'])
        
        for util1, util2 in incompatible_utilities:
            if util1 in utilities_in_path and util2 in utilities_in_path:
                errors.append({
                    'test_code': 'UTIL_003',
                    'severity': ValidationSeverity.HIGH.value,
                    'error_type': 'INCOMPATIBLE_UTILITIES',
                    'error_scope': 'UTILITY',
                    'object_type': 'PATH',
                    'object_ids': [],
                    'error_message': f'Incompatible utilities {util1} and {util2} found in same path',
                    'error_data': f'utilities_in_path={sorted(utilities_in_path)}'
                })
        
        return errors

    def _validate_utility_flow_direction(self, path: Dict[str, Any], 
                                        utility_sequence: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate utility flow direction consistency."""
        errors = []
        
        # This is a simplified validation - real implementation would need
        # more sophisticated flow direction analysis
        
        # Check if path has consistent flow direction based on PoC flow indicators
        # This would require additional PoC flow information from the database
        
        return errors

    def _check_isolated_nodes(self, node_ids: List[int]) -> List[int]:
        """Check for nodes that appear isolated in the network."""
        if not node_ids:
            return []
        
        isolated_nodes = []
        
        for node_id in node_ids:
            # Check if node has any connections
            sql = '''
            SELECT COUNT(*)
            FROM nw_links l
            WHERE l.start_node_id = ? OR l.end_node_id = ?
            '''
            
            rows = self.db.query(sql, [node_id, node_id])
            connection_count = rows[0][0] if rows else 0
            
            if connection_count == 0:
                isolated_nodes.append(node_id)
        
        return isolated_nodes

    def _validate_path_length(self, path: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate path length reasonableness."""
        errors = []
        
        total_length = path.get('total_length_mm', 0)
        node_count = path.get('node_count', 0)
        
        # Define reasonable length thresholds
        max_reasonable_length = 100000  # 100 meters in mm
        min_length_per_segment = 10     # 10mm minimum per segment
        
        if total_length > max_reasonable_length:
            errors.append({
                'test_code': 'STRUCT_002',
                'severity': ValidationSeverity.MEDIUM.value,
                'error_type': 'EXCESSIVE_PATH_LENGTH',
                'error_scope': 'STRUCTURAL',
                'object_type': 'PATH',
                'object_ids': [],
                'error_message': f'Path length {total_length}mm exceeds reasonable maximum',
                'error_data': f'length={total_length};max_reasonable={max_reasonable_length}'
            })
        
        if node_count > 1 and total_length < (node_count - 1) * min_length_per_segment:
            errors.append({
                'test_code': 'STRUCT_003',
                'severity': ValidationSeverity.LOW.value,
                'error_type': 'SUSPICIOUS_SHORT_PATH',
                'error_scope': 'STRUCTURAL',
                'object_type': 'PATH',
                'object_ids': [],
                'error_message': f'Path length {total_length}mm seems too short for {node_count} nodes',
                'error_data': f'length={total_length};nodes={node_count};avg_segment={total_length/(node_count-1) if node_count > 1 else 0}'
            })
        
        return errors

    def _check_circular_references(self, node_ids: List[int]) -> List[Dict[str, Any]]:
        """Check for circular references in the path."""
        errors = []
        
        # Check for duplicate nodes in path (excluding intentional loops)
        seen_nodes = set()
        duplicate_nodes = []
        
        for node_id in node_ids:
            if node_id in seen_nodes:
                duplicate_nodes.append(node_id)
            seen_nodes.add(node_id)
        
        if duplicate_nodes:
            errors.append({
                'test_code': 'STRUCT_004',
                'severity': ValidationSeverity.MEDIUM.value,
                'error_type': 'CIRCULAR_REFERENCE',
                'error_scope': 'STRUCTURAL',
                'object_type': 'NODE',
                'object_ids': duplicate_nodes,
                'error_message': f'Circular references detected: nodes {duplicate_nodes} appear multiple times',
                'error_data': f'duplicate_nodes={duplicate_nodes};path_length={len(node_ids)}'
            })
        
        return errors

    def _parse_path_context(self, path_context: str) -> Dict[str, List[int]]:
        """Parse path context string to extract nodes and links."""
        result = {'nodes': [], 'links': []}
        
        if not path_context:
            return result
        
        try:
            parts = path_context.split(';')
            for part in parts:
                if part.startswith('nodes:'):
                    node_str = part[6:]  # Remove 'nodes:' prefix
                    if node_str:
                        result['nodes'] = [int(x) for x in node_str.split(',') if x.strip()]
                elif part.startswith('links:'):
                    link_str = part[6:]  # Remove 'links:' prefix
                    if link_str:
                        result['links'] = [int(x) for x in link_str.split(',') if x.strip()]
        except (ValueError, IndexError):
            pass
        
        return result

    def _load_validation_tests(self):
        """Load validation test definitions from database."""
        sql = 'SELECT code, name, scope, severity, test_type, description FROM tb_validation_tests WHERE is_active = 1'
        
        rows = self.db.query(sql, [])
        
        for row in rows:
            self.validation_tests[row[0]] = {
                'name': row[1],
                'scope': row[2],
                'severity': row[3],
                'test_type': row[4],
                'description': row[5]
            }

    def _fetch_run_paths(self, run_id: str) -> List[Dict[str, Any]]:
        """Fetch all unique paths for a run."""
        sql = '''
        SELECT DISTINCT pd.id, pd.path_hash, pd.path_context, pd.node_count,
               pd.link_count, pd.total_length_mm, pd.scope
        FROM tb_path_definitions pd
        JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
        WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        
        return [
            {
                'path_definition_id': row[0],
                'path_hash': row[1],
                'path_context': row[2],
                'node_count': row[3],
                'link_count': row[4],
                'total_length_mm': row[5],
                'scope': row[6]
            }
            for row in rows
        ]

    def _store_validation_error(self, run_id: str, path_definition_id: int, error: Dict[str, Any]):
        """Store validation error in database."""
        sql = '''
        INSERT INTO tb_validation_errors (
            run_id, path_definition_id, validation_test_id, severity,
            error_scope, error_type, object_type, object_id, object_guid,
            error_message, error_data, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        # Use first object ID if available, otherwise 0
        object_id = error['object_ids'][0] if error['object_ids'] else 0
        object_guid = f"{error['object_type'].lower()}_{object_id}"
        
        params = [
            run_id, path_definition_id, None,  # validation_test_id can be null for now
            error['severity'], error['error_scope'], error['error_type'],
            error['object_type'], object_id, object_guid,
            error['error_message'], error['error_data'], datetime.now()
        ]
        
        self.db.update(sql, params)

    def _create_review_flag(self, run_id: str, path: Dict[str, Any], error: Dict[str, Any]):
        """Create review flag for critical validation issues."""
        sql = '''
        INSERT INTO tb_review_flags (
            run_id, flag_type, severity, reason, object_type,
            object_id, object_guid, flag_data, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        object_id = error['object_ids'][0] if error['object_ids'] else 0
        object_guid = f"{error['object_type'].lower()}_{object_id}"
        
        params = [
            run_id, 'VALIDATION_ERROR', error['severity'],
            error['error_message'], error['object_type'],
            object_id, object_guid, error['error_data'], datetime.now()
        ]
        
        self.db.update(sql, params)

    def _update_validation_results(self, results: Dict[str, Any], error: Dict[str, Any]):
        """Update validation results with error information."""
        results['total_errors'] += 1
        
        if error['severity'] == 'CRITICAL':
            results['critical_errors'] += 1
        
        # Update severity counts
        severity = error['severity']
        if severity not in results['errors_by_severity']:
            results['errors_by_severity'][severity] = 0
        results['errors_by_severity'][severity] += 1
        
        # Update type counts
        error_type = error['error_type']
        if error_type not in results['errors_by_type']:
            results['errors_by_type'][error_type] = 0
        results['errors_by_type'][error_type] += 1

    def _generate_validation_summary(self, run_id: str) -> Dict[str, Any]:
        """Generate validation summary for the run."""
        
        # Count errors by severity
        severity_sql = '''
        SELECT severity, COUNT(*)
        FROM tb_validation_errors
        WHERE run_id = ?
        GROUP BY severity
        '''
        
        severity_rows = self.db.query(severity_sql, [run_id])
        errors_by_severity = {row[0]: row[1] for row in severity_rows}
        
        # Count errors by type
        type_sql = '''
        SELECT error_type, COUNT(*)
        FROM tb_validation_errors
        WHERE run_id = ?
        GROUP BY error_type
        '''
        
        type_rows = self.db.query(type_sql, [run_id])
        errors_by_type = {row[0]: row[1] for row in type_rows}
        
        # Count review flags
        flags_sql = 'SELECT COUNT(*) FROM tb_review_flags WHERE run_id = ?'
        flags_rows = self.db.query(flags_sql, [run_id])
        total_flags = flags_rows[0][0] if flags_rows else 0
        
        return {
            'errors_by_severity': errors_by_severity,
            'errors_by_type': errors_by_type,
            'total_review_flags': total_flags,
            'validation_passed': len(errors_by_severity) == 0,
            'critical_issues': errors_by_severity.get('CRITICAL', 0),
            'high_priority_issues': errors_by_severity.get('HIGH', 0)
        }

    def fetch_validation_errors(self, run_id: Optional[str] = None, 
                               severity: Optional[str] = None,
                               error_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch validation errors with optional filters."""
        filters = {}
        
        if run_id:
            filters['run_id'] = ('=', run_id)
        if severity:
            filters['severity'] = ('=', severity)
        if error_type:
            filters['error_type'] = ('=', error_type)
        
        base_sql = '''
        SELECT id, run_id, path_definition_id, severity, error_scope,
               error_type, object_type, object_id, object_guid,
               error_message, error_data, created_at
        FROM tb_validation_errors
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY created_at DESC'
        
        rows = self.db.query(sql, params)
        
        return [
            {
                'id': row[0],
                'run_id': row[1],
                'path_definition_id': row[2],
                'severity': row[3],
                'error_scope': row[4],
                'error_type': row[5],
                'object_type': row[6],
                'object_id': row[7],
                'object_guid': row[8],
                'error_message': row[9],
                'error_data': row[10],
                'created_at': row[11]
            }
            for row in rows
        ]

    def fetch_review_flags(self, run_id: Optional[str] = None,
                          flag_type: Optional[str] = None,
                          severity: Optional[str] = None,
                          status: str = 'OPEN') -> List[Dict[str, Any]]:
        """Fetch review flags with optional filters."""
        filters = {'status': ('=', status)}
        
        if run_id:
            filters['run_id'] = ('=', run_id)
        if flag_type:
            filters['flag_type'] = ('=', flag_type)
        if severity:
            filters['severity'] = ('=', severity)
        
        base_sql = '''
        SELECT id, run_id, flag_type, severity, reason, object_type,
               object_id, object_guid, flag_data, status, created_at
        FROM tb_review_flags
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        sql = base_sql + where_clause + ' ORDER BY created_at DESC'
        
        rows = self.db.query(sql, params)
        
        return [
            {
                'id': row[0],
                'run_id': row[1],
                'flag_type': row[2],
                'severity': row[3],
                'reason': row[4],
                'object_type': row[5],
                'object_id': row[6],
                'object_guid': row[7],
                'flag_data': row[8],
                'status': row[9],
                'created_at': row[10]
            }
            for row in rows
        ]

    def update_review_flag_status(self, flag_id: int, status: str, 
                                 resolution_notes: Optional[str] = None) -> int:
        """Update review flag status and resolution."""
        if status in ['RESOLVED', 'DISMISSED']:
            sql = '''
            UPDATE tb_review_flags 
            SET status = ?, resolved_at = ?, resolution_notes = ?
            WHERE id = ?
            '''
            params = [status, datetime.now(), resolution_notes, flag_id]
        else:
            sql = 'UPDATE tb_review_flags SET status = ? WHERE id = ?'
            params = [status, flag_id]
        
        return self.db.update(sql, params)

    def get_validation_statistics(self, run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive validation statistics."""
        filters = {}
        if run_id:
            filters['run_id'] = ('=', run_id)
        
        # Error statistics
        error_base_sql = '''
        SELECT 
            COUNT(*) as total_errors,
            COUNT(CASE WHEN severity = 'CRITICAL' THEN 1 END) as critical_errors,
            COUNT(CASE WHEN severity = 'HIGH' THEN 1 END) as high_errors,
            COUNT(CASE WHEN severity = 'MEDIUM' THEN 1 END) as medium_errors,
            COUNT(CASE WHEN severity = 'LOW' THEN 1 END) as low_errors
        FROM tb_validation_errors
        '''
        
        where_clause, params = StringHelper.build_where_clause(filters)
        error_sql = error_base_sql + where_clause
        
        error_rows = self.db.query(error_sql, params)
        
        # Review flag statistics
        flag_base_sql = '''
        SELECT 
            COUNT(*) as total_flags,
            COUNT(CASE WHEN status = 'OPEN' THEN 1 END) as open_flags,
            COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved_flags
        FROM tb_review_flags
        '''
        
        flag_sql = flag_base_sql + where_clause
        flag_rows = self.db.query(flag_sql, params)
        
        error_stats = error_rows[0] if error_rows else [0] * 5
        flag_stats = flag_rows[0] if flag_rows else [0] * 3
        
        return {
            'total_errors': error_stats[0],
            'critical_errors': error_stats[1],
            'high_errors': error_stats[2],
            'medium_errors': error_stats[3],
            'low_errors': error_stats[4],
            'total_flags': flag_stats[0],
            'open_flags': flag_stats[1],
            'resolved_flags': flag_stats[2],
            'error_rate': error_stats[0] / max(1, self._get_total_paths(run_id)) * 100
        }

    def validate_single_path(self, path_definition_id: int) -> Dict[str, Any]:
        """Validate a single path and return detailed results."""
        
        # Get path information
        path_sql = 'SELECT * FROM tb_path_definitions WHERE id = ?'
        path_rows = self.db.query(path_sql, [path_definition_id])
        
        if not path_rows:
            return {'error': 'Path not found'}
        
        path = {
            'path_definition_id': path_rows[0][0],
            'path_hash': path_rows[0][1],
            'path_context': path_rows[0][19],  # path_context column
            'node_count': path_rows[0][12],
            'link_count': path_rows[0][13],
            'total_length_mm': path_rows[0][14],
            'scope': path_rows[0][3]
        }
        
        # Run validations
        connectivity_errors = self._validate_connectivity('single_path_validation', path)
        utility_errors = self._validate_utility_consistency('single_path_validation', path)
        structural_errors = self._validate_structural_integrity('single_path_validation', path)
        
        all_errors = connectivity_errors + utility_errors + structural_errors
        
        # Categorize errors
        errors_by_severity = {}
        errors_by_type = {}
        
        for error in all_errors:
            severity = error['severity']
            error_type = error['error_type']
            
            errors_by_severity[severity] = errors_by_severity.get(severity, 0) + 1
            errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1
        
        return {
            'path_definition_id': path_definition_id,
            'validation_passed': len(all_errors) == 0,
            'total_errors': len(all_errors),
            'errors': all_errors,
            'errors_by_severity': errors_by_severity,
            'errors_by_type': errors_by_type,
            'critical_errors': errors_by_severity.get('CRITICAL', 0),
            'high_priority_errors': errors_by_severity.get('HIGH', 0)
        }

    def _get_total_paths(self, run_id: Optional[str] = None) -> int:
        """Get total number of paths for error rate calculation."""
        if run_id:
            sql = '''
            SELECT COUNT(DISTINCT pd.id)
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
            '''
            rows = self.db.query(sql, [run_id])
        else:
            sql = 'SELECT COUNT(*) FROM tb_path_definitions'
            rows = self.db.query(sql, [])
        
        return rows[0][0] if rows else 1