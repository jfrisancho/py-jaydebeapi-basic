# managers/validation.py

import json
from typing import Dict, List, Optional, Set, Tuple, Any
from datetime import datetime
from enum import Enum

from db import Database
from string_helper import StringHelper


class ValidationSeverity(Enum):
    """Validation severity levels"""
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class ValidationScope(Enum):
    """Validation scope categories"""
    FLOW = 'FLOW'
    CONNECTIVITY = 'CONNECTIVITY'
    MATERIAL = 'MATERIAL'
    QA = 'QA'
    SCENARIO = 'SCENARIO'


class ValidationTestType(Enum):
    """Validation test types"""
    STRUCTURAL = 'STRUCTURAL'
    LOGICAL = 'LOGICAL'
    PERFORMANCE = 'PERFORMANCE'
    COMPLIANCE = 'COMPLIANCE'


class ValidationManager:
    """
    Comprehensive path validation manager.
    Implements connectivity and utility consistency validation with extensible framework.
    """
    
    def __init__(self, db: Database):
        self.db = db
        self._validation_tests = {}  # Cache for validation tests
        self._utility_compatibility = {}  # Cache for utility compatibility rules
        
    def validate_run_paths(self, run_id: str) -> Dict[str, Any]:
        """
        Validate all paths found during a run.
        
        Args:
            run_id: Run identifier
            
        Returns:
            Dictionary with validation summary metrics
        """
        print(f'Starting validation for run {run_id}')
        
        # Load validation tests
        self._load_validation_tests()
        
        # Load utility compatibility rules
        self._load_utility_compatibility()
        
        # Get all paths for this run
        paths = self._fetch_run_paths(run_id)
        
        validation_summary = {
            'total_paths': len(paths),
            'total_errors': 0,
            'total_review_flags': 0,
            'critical_errors': 0,
            'validation_by_severity': {
                'LOW': 0,
                'MEDIUM': 0,
                'HIGH': 0,
                'CRITICAL': 0
            },
            'validation_by_scope': {
                'CONNECTIVITY': 0,
                'FLOW': 0,
                'MATERIAL': 0,
                'QA': 0
            }
        }
        
        # Validate each path
        for path_info in paths:
            path_errors = self._validate_single_path(run_id, path_info)
            
            validation_summary['total_errors'] += len(path_errors)
            
            # Count by severity
            for error in path_errors:
                severity = error.get('severity', 'MEDIUM')
                validation_summary['validation_by_severity'][severity] += 1
                
                if severity == 'CRITICAL':
                    validation_summary['critical_errors'] += 1
                
                # Count by scope
                scope = error.get('error_scope', 'QA')
                if scope in validation_summary['validation_by_scope']:
                    validation_summary['validation_by_scope'][scope] += 1
        
        print(f'Validation completed: {validation_summary["total_errors"]} errors found across {len(paths)} paths')
        
        return validation_summary
    
    def _validate_single_path(self, run_id: str, path_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Validate a single path with all applicable tests.
        
        Args:
            run_id: Run identifier
            path_info: Path information dictionary
            
        Returns:
            List of validation errors found
        """
        path_definition_id = path_info['path_definition_id']
        path_context = path_info.get('path_context', {})
        
        errors = []
        
        # 1. Connectivity validation
        connectivity_errors = self._validate_connectivity(run_id, path_definition_id, path_context)
        errors.extend(connectivity_errors)
        
        # 2. Utility consistency validation
        utility_errors = self._validate_utility_consistency(run_id, path_definition_id, path_context)
        errors.extend(utility_errors)
        
        # 3. Structural validation
        structural_errors = self._validate_path_structure(run_id, path_definition_id, path_context)
        errors.extend(structural_errors)
        
        # 4. PoC validation
        poc_errors = self._validate_poc_requirements(run_id, path_definition_id, path_context)
        errors.extend(poc_errors)
        
        return errors
    
    def _validate_connectivity(self, run_id: str, path_definition_id: int, path_context: Dict) -> List[Dict[str, Any]]:
        """
        Validate path connectivity and completeness.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition ID
            path_context: Path context with nodes and links
            
        Returns:
            List of connectivity validation errors
        """
        errors = []
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        
        if not nodes:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY_001',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'STRUCTURAL',
                'PATH', 0, '', 'Empty path - no nodes found'
            ))
            return errors
        
        # Check node connectivity
        for i in range(len(nodes) - 1):
            current_node = nodes[i]
            next_node = nodes[i + 1]
            
            # Verify there's a link between consecutive nodes
            if not self._verify_node_connection(current_node, next_node, links):
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'CONNECTIVITY_002',
                    ValidationSeverity.HIGH, 'CONNECTIVITY', 'STRUCTURAL',
                    'NODE', current_node, '', 
                    f'Missing connection between nodes {current_node} and {next_node}'
                ))
        
        # Check for orphaned links
        orphaned_links = self._find_orphaned_links(nodes, links)
        for link_id in orphaned_links:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONNECTIVITY_003',
                ValidationSeverity.MEDIUM, 'CONNECTIVITY', 'STRUCTURAL',
                'LINK', link_id, '', 
                f'Orphaned link {link_id} not connected to path nodes'
            ))
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path_definition_id: int, path_context: Dict) -> List[Dict[str, Any]]:
        """
        Validate utility consistency along the path.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition ID
            path_context: Path context with nodes and links
            
        Returns:
            List of utility validation errors
        """
        errors = []
        nodes = path_context.get('nodes', [])
        
        if len(nodes) < 2:
            return errors
        
        # Get utility information for all nodes in path
        node_utilities = self._fetch_node_utilities(nodes)
        
        # Track utility changes along path
        current_utility = None
        utility_changes = []
        
        for node_id in nodes:
            node_utility = node_utilities.get(node_id)
            
            if node_utility is not None:
                if current_utility is None:
                    current_utility = node_utility
                elif current_utility != node_utility:
                    # Utility change detected
                    if self._is_valid_utility_transition(current_utility, node_utility):
                        utility_changes.append({
                            'from': current_utility,
                            'to': node_utility,
                            'at_node': node_id
                        })
                        current_utility = node_utility
                    else:
                        # Invalid utility transition
                        errors.append(self._create_validation_error(
                            run_id, path_definition_id, 'UTILITY_001',
                            ValidationSeverity.HIGH, 'FLOW', 'LOGICAL',
                            'NODE', node_id, '', 
                            f'Invalid utility transition from {current_utility} to {node_utility}',
                            {'from_utility': current_utility, 'to_utility': node_utility}
                        ))
        
        # Validate PoC utilities match connected path utilities
        start_poc_id = path_context.get('start_poc_id')
        end_poc_id = path_context.get('end_poc_id')
        
        if start_poc_id and end_poc_id:
            poc_errors = self._validate_poc_utility_consistency(
                run_id, path_definition_id, start_poc_id, end_poc_id, 
                node_utilities, nodes
            )
            errors.extend(poc_errors)
        
        return errors
    
    def _validate_path_structure(self, run_id: str, path_definition_id: int, path_context: Dict) -> List[Dict[str, Any]]:
        """
        Validate path structural integrity.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition ID
            path_context: Path context with nodes and links
            
        Returns:
            List of structural validation errors
        """
        errors = []
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        
        # Check for duplicate nodes (cycles)
        node_counts = {}
        for node_id in nodes:
            node_counts[node_id] = node_counts.get(node_id, 0) + 1
        
        for node_id, count in node_counts.items():
            if count > 1:
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'STRUCTURE_001',
                    ValidationSeverity.MEDIUM, 'CONNECTIVITY', 'STRUCTURAL',
                    'NODE', node_id, '', 
                    f'Node {node_id} appears {count} times in path (potential cycle)'
                ))
        
        # Check path length reasonableness
        if len(nodes) > 1000:  # Configurable threshold
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'STRUCTURE_002',
                ValidationSeverity.LOW, 'QA', 'PERFORMANCE',
                'PATH', 0, '', 
                f'Path unusually long with {len(nodes)} nodes'
            ))
        
        # Check for isolated segments
        if len(links) > 0 and len(nodes) > 2:
            isolated_segments = self._find_isolated_segments(nodes, links)
            for segment in isolated_segments:
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'STRUCTURE_003',
                    ValidationSeverity.HIGH, 'CONNECTIVITY', 'STRUCTURAL',
                    'PATH', 0, '', 
                    f'Isolated path segment detected: {segment}'
                ))
        
        return errors
    
    def _validate_poc_requirements(self, run_id: str, path_definition_id: int, path_context: Dict) -> List[Dict[str, Any]]:
        """
        Validate PoC requirements and metadata.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition ID
            path_context: Path context with PoC information
            
        Returns:
            List of PoC validation errors
        """
        errors = []
        
        start_poc_id = path_context.get('start_poc_id')
        end_poc_id = path_context.get('end_poc_id')
        
        if not start_poc_id or not end_poc_id:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_001',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'STRUCTURAL',
                'PATH', 0, '', 
                'Missing PoC information in path context'
            ))
            return errors
        
        # Validate start PoC
        start_poc_errors = self._validate_single_poc(run_id, path_definition_id, start_poc_id, 'start')
        errors.extend(start_poc_errors)
        
        # Validate end PoC
        end_poc_errors = self._validate_single_poc(run_id, path_definition_id, end_poc_id, 'end')
        errors.extend(end_poc_errors)
        
        return errors
    
    def _validate_single_poc(self, run_id: str, path_definition_id: int, poc_id: int, position: str) -> List[Dict[str, Any]]:
        """
        Validate a single PoC's requirements.
        
        Args:
            run_id: Run identifier
            path_definition_id: Path definition ID
            poc_id: PoC identifier
            position: 'start' or 'end'
            
        Returns:
            List of PoC validation errors
        """
        errors = []
        
        # Fetch PoC information
        poc_info = self._fetch_poc_info(poc_id)
        
        if not poc_info:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_002',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'STRUCTURAL',
                'POC', poc_id, '', 
                f'PoC {poc_id} not found in database'
            ))
            return errors
        
        # Check required fields
        required_checks = [
            ('utility_no', 'POC_003', 'Missing utility number'),
            ('markers', 'POC_004', 'Missing markers'),
            ('reference', 'POC_005', 'Missing reference')
        ]
        
        for field, error_code, message in required_checks:
            if not poc_info.get(field):
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, error_code,
                    ValidationSeverity.HIGH, 'CONNECTIVITY', 'COMPLIANCE',
                    'POC', poc_id, poc_info.get('markers', ''), 
                    f'{message} for {position} PoC {poc_id}',
                    poc_info
                ))
        
        # Check if PoC is marked as used
        if not poc_info.get('is_used'):
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_006',
                ValidationSeverity.MEDIUM, 'QA', 'LOGICAL',
                'POC', poc_id, poc_info.get('markers', ''), 
                f'{position.capitalize()} PoC {poc_id} is not marked as used but has connectivity'
            ))
        
        return errors
    
    def flag_connectivity_issue(self, run_id: str, start_poc: Dict, end_poc: Dict, reason: str) -> None:
        """
        Flag a connectivity issue for manual review.
        
        Args:
            run_id: Run identifier
            start_poc: Start PoC information
            end_poc: End PoC information
            reason: Reason for flagging
        """
        flag_data = {
            'start_poc': start_poc,
            'end_poc': end_poc,
            'analysis_date': datetime.now().isoformat()
        }
        
        sql = '''
            INSERT INTO tb_review_flags (
                run_id, flag_type, severity, reason,
                object_type, object_id, object_guid,
                object_utility_no, object_markers,
                flag_data, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            run_id,
            'CONNECTIVITY_ISSUE',
            ValidationSeverity.HIGH.value,
            reason,
            'POC',
            start_poc['id'],
            start_poc.get('equipment', {}).get('guid', ''),
            start_poc.get('utility_no'),
            start_poc.get('markers'),
            json.dumps(flag_data),
            'OPEN',
            datetime.now()
        ]
        
        self.db.update(sql, params)
    
    def fetch_validation_errors(self, run_id: str, severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch validation errors for a run.
        
        Args:
            run_id: Run identifier
            severity: Optional severity filter
            
        Returns:
            List of validation error dictionaries
        """
        filters = {'run_id': ('=', run_id)}
        
        if severity:
            filters['severity'] = ('=', severity)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id, run_id, path_definition_id, validation_test_id,
                   severity, error_scope, error_type,
                   object_type, object_id, object_guid,
                   object_utility_no, object_markers,
                   error_message, error_data, created_at, notes
            FROM tb_validation_errors
            {where_clause}
            ORDER BY severity DESC, created_at DESC
        '''
        
        rows = self.db.query(sql, params)
        
        errors = []
        for row in rows:
            error_data = {}
            try:
                if row[13]:  # error_data
                    error_data = json.loads(row[13])
            except json.JSONDecodeError:
                pass
            
            errors.append({
                'id': row[0],
                'run_id': row[1],
                'path_definition_id': row[2],
                'validation_test_id': row[3],
                'severity': row[4],
                'error_scope': row[5],
                'error_type': row[6],
                'object_type': row[7],
                'object_id': row[8],
                'object_guid': row[9],
                'object_utility_no': row[10],
                'object_markers': row[11],
                'error_message': row[12],
                'error_data': error_data,
                'created_at': row[14],
                'notes': row[15]
            })
        
        return errors
    
    def fetch_review_flags(self, run_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch review flags for a run.
        
        Args:
            run_id: Run identifier
            status: Optional status filter
            
        Returns:
            List of review flag dictionaries
        """
        filters = {'run_id': ('=', run_id)}
        
        if status:
            filters['status'] = ('=', status)
        
        where_clause, params = StringHelper.build_where_clause(filters)
        
        sql = f'''
            SELECT id, run_id, flag_type, severity, reason,
                   object_type, object_id, object_guid,
                   object_utility_no, object_markers,
                   flag_data, status, assigned_to, resolved_at,
                   resolution_notes, created_at, notes
            FROM tb_review_flags
            {where_clause}
            ORDER BY severity DESC, created_at DESC
        '''
        
        rows = self.db.query(sql, params)
        
        flags = []
        for row in rows:
            flag_data = {}
            try:
                if row[10]:  # flag_data
                    flag_data = json.loads(row[10])
            except json.JSONDecodeError:
                pass
            
            flags.append({
                'id': row[0],
                'run_id': row[1],
                'flag_type': row[2],
                'severity': row[3],
                'reason': row[4],
                'object_type': row[5],
                'object_id': row[6],
                'object_guid': row[7],
                'object_utility_no': row[8],
                'object_markers': row[9],
                'flag_data': flag_data,
                'status': row[11],
                'assigned_to': row[12],
                'resolved_at': row[13],
                'resolution_notes': row[14],
                'created_at': row[15],
                'notes': row[16]
            })
        
        return flags
    
    def _load_validation_tests(self) -> None:
        """Load validation test definitions."""
        sql = '''
            SELECT code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests
            WHERE is_active = 1
        '''
        
        rows = self.db.query(sql)
        
        for row in rows:
            self._validation_tests[row[0]] = {
                'code': row[0],
                'name': row[1],
                'scope': row[2],
                'severity': row[3],
                'test_type': row[4],
                'is_active': row[5],
                'description': row[6]
            }
    
    def _load_utility_compatibility(self) -> None:
        """Load utility compatibility rules."""
        # This is a placeholder - in practice, you'd load from a configuration table
        # or external configuration file
        self._utility_compatibility = {
            # Water utilities
            'PW': ['PW', 'WS'],  # Process water can transition to waste
            'DI': ['DI', 'WS'],  # Deionized water can transition to waste
            'WS': [],  # Waste water is terminal
            
            # Gas utilities
            'N2': ['N2'],  # Nitrogen stays nitrogen
            'CDA': ['CDA'],  # Clean dry air stays CDA
            'AR': ['AR'],  # Argon stays argon
            
            # Special cases for phase changes
            'STEAM': ['PW', 'WS'],  # Steam can condense to water
        }
    
    def _fetch_run_paths(self, run_id: str) -> List[Dict[str, Any]]:
        """Fetch all paths for a run with their contexts."""
        sql = '''
            SELECT pd.id as path_definition_id, pd.path_hash, pd.path_context,
                   ap.start_node_id, ap.end_node_id
            FROM tb_path_definitions pd
            JOIN tb_attempt_paths ap ON pd.id = ap.path_definition_id
            WHERE ap.run_id = ?
        '''
        
        rows = self.db.query(sql, [run_id])
        
        paths = []
        for row in rows:
            path_context = {}
            try:
                if row[2]:  # path_context
                    path_context = json.loads(row[2])
            except json.JSONDecodeError:
                pass
            
            paths.append({
                'path_definition_id': row[0],
                'path_hash': row[1],
                'path_context': path_context,
                'start_node_id': row[3],
                'end_node_id': row[4]
            })
        
        return paths
    
    def _verify_node_connection(self, node1: int, node2: int, path_links: List[int]) -> bool:
        """Verify that two nodes are connected by a link in the path."""
        sql = '''
            SELECT COUNT(*)
            FROM nw_links
            WHERE id IN (''' + ','.join(['?'] * len(path_links)) + ''')
            AND ((start_node_id = ? AND end_node_id = ?) 
                 OR (start_node_id = ? AND end_node_id = ? AND bidirected = 'Y'))
        '''
        
        params = path_links + [node1, node2, node2, node1]
        result = self.db.query(sql, params)
        
        return result[0][0] > 0 if result else False
    
    def _find_orphaned_links(self, nodes: List[int], links: List[int]) -> List[int]:
        """Find links that don't connect to any nodes in the path."""
        if not links:
            return []
        
        placeholders_links = ','.join(['?'] * len(links))
        placeholders_nodes = ','.join(['?'] * len(nodes))
        
        sql = f'''
            SELECT id
            FROM nw_links
            WHERE id IN ({placeholders_links})
            AND NOT (start_node_id IN ({placeholders_nodes}) 
                     OR end_node_id IN ({placeholders_nodes}))
        '''
        
        params = links + nodes + nodes
        rows = self.db.query(sql, params)
        
        return [row[0] for row in rows]
    
    def _fetch_node_utilities(self, nodes: List[int]) -> Dict[int, int]:
        """Fetch utility information for nodes."""
        if not nodes:
            return {}
        
        placeholders = ','.join(['?'] * len(nodes))
        sql = f'''
            SELECT id, utility_no
            FROM nw_nodes
            WHERE id IN ({placeholders})
            AND utility_no IS NOT NULL
        '''
        
        rows = self.db.query(sql, nodes)
        return {row[0]: row[1] for row in rows}
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if utility transition is valid."""
        # Convert to string keys for lookup
        from_key = str(from_utility)
        to_key = str(to_utility)
        
        # If no compatibility rules defined, allow any transition
        if from_key not in self._utility_compatibility:
            return True
        
        allowed_transitions = self._utility_compatibility[from_key]
        return to_key in allowed_transitions
    
    def _validate_poc_utility_consistency(self, run_id: str, path_definition_id: int,
                                        start_poc_id: int, end_poc_id: int,
                                        node_utilities: Dict[int, int], nodes: List[int]) -> List[Dict[str, Any]]:
        """Validate PoC utilities match connected path utilities."""
        errors = []
        
        # Get PoC utilities
        poc_utilities = self._fetch_poc_utilities([start_poc_id, end_poc_id])
        
        start_poc_utility = poc_utilities.get(start_poc_id)
        end_poc_utility = poc_utilities.get(end_poc_id)
        
        # Get first and last node utilities
        first_node_utility = node_utilities.get(nodes[0]) if nodes else None
        last_node_utility = node_utilities.get(nodes[-1]) if nodes else None
        
        # Check start PoC consistency
        if start_poc_utility and first_node_utility and start_poc_utility != first_node_utility:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'UTILITY_002',
                ValidationSeverity.HIGH, 'FLOW', 'LOGICAL',
                'POC', start_poc_id, '', 
                f'Start PoC utility {start_poc_utility} does not match first node utility {first_node_utility}'
            ))
        
        # Check end PoC consistency
        if end_poc_utility and last_node_utility and end_poc_utility != last_node_utility:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'UTILITY_003',
                ValidationSeverity.HIGH, 'FLOW', 'LOGICAL',
                'POC', end_poc_id, '', 
                f'End PoC utility {end_poc_utility} does not match last node utility {last_node_utility}'
            ))
        
        return errors
    
    def _find_isolated_segments(self, nodes: List[int], links: List[int]) -> List[List[int]]:
        """Find isolated segments in the path."""
        # This is a simplified implementation
        # In practice, you might want more sophisticated graph analysis
        return []  # Placeholder
    
    def _fetch_poc_info(self, poc_id: int) -> Optional[Dict[str, Any]]:
        """Fetch PoC information."""
        sql = '''
            SELECT id, node_id, is_used, markers, utility_no, reference, flow, is_loopback
            FROM tb_equipment_pocs
            WHERE id = ?
        '''
        
        rows = self.db.query(sql, [poc_id])
        if not rows:
            return None
        
        row = rows[0]
        return {
            'id': row[0],
            'node_id': row[1],
            'is_used': row[2],
            'markers': row[3],
            'utility_no': row[4],
            'reference': row[5],
            'flow': row[6],
            'is_loopback': row[7]
        }
    
    def _fetch_poc_utilities(self, poc_ids: List[int]) -> Dict[int, int]:
        """Fetch utility information for PoCs."""
        if not poc_ids:
            return {}
        
        placeholders = ','.join(['?'] * len(poc_ids))
        sql = f'''
            SELECT id, utility_no
            FROM tb_equipment_pocs
            WHERE id IN ({placeholders})
            AND utility_no IS NOT NULL
        '''
        
        rows = self.db.query(sql, poc_ids)
        return {row[0]: row[1] for row in rows}
    
    def _create_validation_error(self, run_id: str, path_definition_id: int, error_code: str,
                               severity: ValidationSeverity, error_scope: str, error_type: str,
                               object_type: str, object_id: int, object_guid: str,
                               error_message: str, additional_data: Optional[Dict] = None) -> Dict[str, Any]:
        """Create and store a validation error."""
        
        error_data = json.dumps(additional_data) if additional_data else None
        
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
            severity.value,
            error_scope,
            error_type,
            object_type,
            object_id,
            object_guid,
            error_message,
            error_data,
            datetime.now()
        ]
        
        self.db.update(sql, params)
        
        return {
            'error_code': error_code,
            'severity': severity.value,
            'error_scope': error_scope,
            'error_type': error_type,
            'object_type': object_type,
            'object_id': object_id,
            'error_message': error_message,
            'additional_data': additional_data
        }