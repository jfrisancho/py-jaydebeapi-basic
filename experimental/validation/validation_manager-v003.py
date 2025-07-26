from dataclasses import dataclass
from collections import Counter
from typing import Any
import json
from datetime import datetime

from database.database import Database
from helpers.string_helper import StringHelper
from validation.validation_data_structures_enums import (
    ValidationScope, Severity, ValidationType, ObjectType, TagType, TagCode
)


@dataclass
class ValidationResult:
    """Result of path validation."""
    passed: bool
    total_errors: int
    total_paths_validated: int
    counts_by_type: Counter[ValidationType]
    counts_by_severity: Counter[Severity]


class ValidationManager:
    """Comprehensive path validation framework."""
    
    def __init__(self, db: Database, verbose: bool = False, silent: bool = False):
        self.db = db
        self.verbose = verbose
        self.silent = silent
        self.validation_tests = self._load_validation_tests()
    
    def validate_run_paths(self, run_id: str) -> dict[str, Any]:
        """
        Validate all paths in a run and return comprehensive results.
        Performs connectivity and utility validation on all paths.
        """
        if not self.silent:
            print(f'Starting validation for run {run_id}')
        
        # Get all path executions for the run
        paths = self._fetch_run_path_executions(run_id)
        
        validation_results = {
            'total_paths_validated': len(paths),
            'total_errors': 0,
            'total_review_flags': 0,
            'critical_errors': 0,
            'errors_by_severity': Counter(),
            'errors_by_type': Counter(),
            'validation_summary': {}
        }
        
        for path_execution in paths:
            path_result = self._validate_single_path(run_id, path_execution)
            
            # Update counters
            validation_results['total_errors'] += path_result['error_count']
            validation_results['critical_errors'] += path_result['critical_count']
            validation_results['total_review_flags'] += path_result['review_flag_count']
            
            for severity, count in path_result['errors_by_severity'].items():
                validation_results['errors_by_severity'][severity] += count
                
            for error_type, count in path_result['errors_by_type'].items():
                validation_results['errors_by_type'][error_type] += count
        
        # Update validation summary in path executions
        self._update_path_execution_validation_status(run_id, validation_results)
        
        if not self.silent:
            self._print_validation_summary(validation_results)
        
        return validation_results
    
    def _validate_single_path(self, run_id: str, path_execution: dict) -> dict[str, Any]:
        """Validate a single path execution."""
        path_id = path_execution['path_id']
        execution_id = path_execution['id']
        
        result = {
            'error_count': 0,
            'critical_count': 0,
            'review_flag_count': 0,
            'errors_by_severity': Counter(),
            'errors_by_type': Counter()
        }
        
        # Parse path context to get nodes and links
        path_context = json.loads(path_execution.get('path_context', '{}'))
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        
        if not nodes or not links:
            self._create_validation_error(
                run_id, execution_id, 'MISSING_PATH_DATA',
                Severity.CRITICAL, 'PATH', path_id, 'Missing path nodes or links data'
            )
            result['error_count'] += 1
            result['critical_count'] += 1
            result['errors_by_severity'][Severity.CRITICAL] += 1
            return result
        
        # Validate connectivity
        connectivity_errors = self._validate_path_connectivity(run_id, execution_id, nodes, links)
        
        # Validate utilities
        utility_errors = self._validate_path_utilities(run_id, execution_id, nodes, links, path_execution)
        
        # Generate AI training tags
        self._generate_path_tags(run_id, path_execution, connectivity_errors + utility_errors)
        
        # Aggregate results
        all_errors = connectivity_errors + utility_errors
        for error in all_errors:
            result['error_count'] += 1
            result['errors_by_severity'][error['severity']] += 1
            result['errors_by_type'][error['error_type']] += 1
            
            if error['severity'] == Severity.CRITICAL:
                result['critical_count'] += 1
                # Create review flag for critical errors
                self._create_review_flag(run_id, error, path_execution)
                result['review_flag_count'] += 1
        
        return result
    
    def _validate_path_connectivity(self, run_id: str, execution_id: int, nodes: list, links: list) -> list[dict]:
        """Validate path connectivity - critical for semiconductor fabrication."""
        errors = []
        
        # Fetch actual node and link data
        node_data = self._fetch_nodes_data(nodes)
        link_data = self._fetch_links_data(links)
        
        # Check for missing nodes
        found_node_ids = {n['id'] for n in node_data}
        missing_nodes = set(nodes) - found_node_ids
        
        for missing_node_id in missing_nodes:
            errors.append(self._create_validation_error(
                run_id, execution_id, 'NOT_FOUND_NODE',
                Severity.CRITICAL, 'NODE', missing_node_id, 
                f'Node {missing_node_id} not found in database'
            ))
        
        # Check for missing links
        found_link_ids = {l['id'] for l in link_data}
        missing_links = set(links) - found_link_ids
        
        for missing_link_id in missing_links:
            errors.append(self._create_validation_error(
                run_id, execution_id, 'NOT_FOUND_LINK',
                Severity.CRITICAL, 'LINK', missing_link_id,
                f'Link {missing_link_id} not found in database'
            ))
        
        # Validate connectivity chain
        if node_data and link_data:
            connectivity_errors = self._validate_connectivity_chain(
                run_id, execution_id, node_data, link_data
            )
            errors.extend(connectivity_errors)
        
        return errors
    
    def _validate_connectivity_chain(self, run_id: str, execution_id: int, nodes: list, links: list) -> list[dict]:
        """Validate that nodes are properly connected through links."""
        errors = []
        
        # Build connectivity map
        node_connections = {}
        for node in nodes:
            node_connections[node['id']] = {'in_links': [], 'out_links': []}
        
        # Map links to nodes
        for link in links:
            start_node = link.get('start_node_id')
            end_node = link.get('end_node_id')
            
            if not start_node or not end_node:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'MISSING_START_END_NODE',
                    Severity.CRITICAL, 'LINK', link['id'],
                    f'Link {link["id"]} missing start or end node'
                ))
                continue
            
            if start_node in node_connections:
                node_connections[start_node]['out_links'].append(link['id'])
            if end_node in node_connections:
                node_connections[end_node]['in_links'].append(link['id'])
        
        # Check for disconnected nodes (except first and last)
        node_ids = list(node_connections.keys())
        for i, node_id in enumerate(node_ids):
            connections = node_connections[node_id]
            
            # First node should have outgoing connections
            if i == 0 and not connections['out_links']:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'DISCONNECTED',
                    Severity.CRITICAL, 'NODE', node_id,
                    f'Start node {node_id} has no outgoing connections'
                ))
            
            # Last node should have incoming connections
            elif i == len(node_ids) - 1 and not connections['in_links']:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'DISCONNECTED',
                    Severity.CRITICAL, 'NODE', node_id,
                    f'End node {node_id} has no incoming connections'
                ))
            
            # Middle nodes should have both incoming and outgoing
            elif 0 < i < len(node_ids) - 1:
                if not connections['in_links'] or not connections['out_links']:
                    errors.append(self._create_validation_error(
                        run_id, execution_id, 'BROKEN_CONTINUITY',
                        Severity.CRITICAL, 'NODE', node_id,
                        f'Node {node_id} breaks path continuity'
                    ))
        
        return errors
    
    def _validate_path_utilities(self, run_id: str, execution_id: int, nodes: list, links: list, path_execution: dict) -> list[dict]:
        """Validate utility consistency along the path."""
        errors = []
        
        # Get utilities scope from path execution
        utilities_scope = path_execution.get('utilities_scope')
        if not utilities_scope:
            return errors
        
        try:
            expected_utilities = json.loads(utilities_scope) if isinstance(utilities_scope, str) else utilities_scope
        except (json.JSONDecodeError, TypeError):
            expected_utilities = []
        
        # Fetch node and link data with utility information
        node_data = self._fetch_nodes_with_utilities(nodes)
        link_data = self._fetch_links_with_utilities(links)
        
        # Validate node utilities
        for node in node_data:
            node_utility = node.get('utility_no')
            if node_utility and str(node_utility) not in expected_utilities:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'UTILITY_MISMATCH',
                    Severity.ERROR, 'NODE', node['id'],
                    f'Node utility {node_utility} not in expected utilities {expected_utilities}'
                ))
        
        # Validate link utilities
        for link in link_data:
            link_utility = link.get('utility_no')
            if link_utility and str(link_utility) not in expected_utilities:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'UTILITY_MISMATCH',
                    Severity.ERROR, 'LINK', link['id'],
                    f'Link utility {link_utility} not in expected utilities {expected_utilities}'
                ))
        
        # Check for utility transitions
        utility_transitions = self._check_utility_transitions(node_data, link_data)
        for transition in utility_transitions:
            if transition['is_invalid']:
                errors.append(self._create_validation_error(
                    run_id, execution_id, 'INVALID_TRANSITION',
                    Severity.WARNING, transition['object_type'], transition['object_id'],
                    f'Invalid utility transition: {transition["description"]}'
                ))
        
        return errors
    
    def _check_utility_transitions(self, nodes: list, links: list) -> list[dict]:
        """Check for invalid utility transitions along the path."""
        transitions = []
        
        # Build sequence of utilities
        utility_sequence = []
        
        # Add nodes and links in path order
        for i, node in enumerate(nodes):
            utility_sequence.append({
                'type': 'NODE',
                'id': node['id'],
                'utility': node.get('utility_no'),
                'position': i * 2
            })
            
            if i < len(links):
                utility_sequence.append({
                    'type': 'LINK',
                    'id': links[i]['id'],
                    'utility': links[i].get('utility_no'),
                    'position': i * 2 + 1
                })
        
        # Check transitions
        for i in range(len(utility_sequence) - 1):
            current = utility_sequence[i]
            next_item = utility_sequence[i + 1]
            
            if current['utility'] != next_item['utility'] and current['utility'] is not None and next_item['utility'] is not None:
                transitions.append({
                    'object_type': next_item['type'],
                    'object_id': next_item['id'],
                    'is_invalid': True,  # For now, mark all utility changes as potentially invalid
                    'description': f'Utility change from {current["utility"]} to {next_item["utility"]}'
                })
        
        return transitions
    
    def _generate_path_tags(self, run_id: str, path_execution: dict, validation_errors: list):
        """Generate AI training tags based on path characteristics and validation results."""
        path_id = path_execution['path_id']
        
        # Generate tags based on path metrics
        self._generate_metric_based_tags(run_id, path_id, path_execution)
        
        # Generate tags based on validation errors
        self._generate_error_based_tags(run_id, path_id, validation_errors)
        
        # Generate utility tags
        self._generate_utility_tags(run_id, path_id, path_execution)
    
    def _generate_metric_based_tags(self, run_id: str, path_id: int, path_execution: dict):
        """Generate tags based on path metrics."""
        coverage = path_execution.get('coverage', 0)
        cost = path_execution.get('cost', 0)
        length_mm = path_execution.get('length_mm', 0)
        node_count = path_execution.get('node_count', 0)
        link_count = path_execution.get('link_count', 0)
        
        # Coverage-based tags
        if coverage < 0.5:
            self._store_path_tag(run_id, path_id, TagType.QA, 'LOW_COVERAGE', 
                               f'Coverage: {coverage:.2%}', 'SYSTEM', 0.9)
        elif coverage > 0.95:
            self._store_path_tag(run_id, path_id, TagType.QA, 'HIGH_COVERAGE',
                               f'Coverage: {coverage:.2%}', 'SYSTEM', 0.9)
        
        # Cost-based tags
        if cost < 0:
            self._store_path_tag(run_id, path_id, TagType.RISK, 'NEGATIVE_COST',
                               f'Cost: {cost}', 'SYSTEM', 1.0)
        elif cost > 10000:  # Adjust threshold as needed
            self._store_path_tag(run_id, path_id, TagType.RISK, 'HIGH_COST',
                               f'Cost: {cost}', 'SYSTEM', 0.8)
        
        # Length-based tags
        if length_mm > 100000:  # 100m threshold
            self._store_path_tag(run_id, path_id, TagType.QA, 'UNUSUAL_LENGTH',
                               f'Length: {length_mm}mm', 'SYSTEM', 0.7)
        
        # Complexity-based tags
        complexity_ratio = node_count / max(link_count, 1)
        if complexity_ratio > 1.5:
            self._store_path_tag(run_id, path_id, TagType.QA, 'HIGH_COMPLEXITY',
                               f'Node/Link ratio: {complexity_ratio:.2f}', 'SYSTEM', 0.8)
    
    def _generate_error_based_tags(self, run_id: str, path_id: int, validation_errors: list):
        """Generate tags based on validation errors."""
        error_types = Counter(error['error_type'] for error in validation_errors)
        
        for error_type, count in error_types.items():
            confidence = min(0.5 + (count * 0.1), 1.0)  # Higher confidence with more errors
            
            if 'CONNECTIVITY' in error_type or 'CONTINUITY' in error_type:
                self._store_path_tag(run_id, path_id, TagType.RISK, 'CONNECTIVITY_ISSUE',
                                   f'{error_type}: {count} errors', 'VALIDATION', confidence)
            
            elif 'UTILITY' in error_type:
                self._store_path_tag(run_id, path_id, TagType.UTILITY, 'UTILITY_ISSUE',
                                   f'{error_type}: {count} errors', 'VALIDATION', confidence)
            
            elif 'MISSING' in error_type:
                self._store_path_tag(run_id, path_id, TagType.QA, 'DATA_QUALITY',
                                   f'{error_type}: {count} errors', 'VALIDATION', confidence)
    
    def _generate_utility_tags(self, run_id: str, path_id: int, path_execution: dict):
        """Generate utility-specific tags for AI training."""
        utilities_scope = path_execution.get('utilities_scope')
        if not utilities_scope:
            return
        
        try:
            utilities = json.loads(utilities_scope) if isinstance(utilities_scope, str) else utilities_scope
            
            # Tag based on number of utilities
            if len(utilities) == 1:
                self._store_path_tag(run_id, path_id, TagType.UTILITY, 'SINGLE_UTILITY',
                                   f'Utility: {utilities[0]}', 'SYSTEM', 1.0)
            elif len(utilities) > 3:
                self._store_path_tag(run_id, path_id, TagType.UTILITY, 'MULTI_UTILITY',
                                   f'Utilities: {len(utilities)}', 'SYSTEM', 0.9)
            
            # Tag specific utility types (customize based on your utility codes)
            for utility in utilities:
                self._store_path_tag(run_id, path_id, TagType.UTILITY, f'UTY_{utility}',
                                   f'Uses utility {utility}', 'SYSTEM', 1.0)
                
        except (json.JSONDecodeError, TypeError):
            self._store_path_tag(run_id, path_id, TagType.QA, 'INVALID_UTILITIES',
                               'Invalid utilities data', 'SYSTEM', 1.0)
    
    def _create_validation_error(self, run_id: str, path_execution_id: int, error_type: str, 
                               severity: Severity, object_type: str, object_id: int, 
                               notes: str = None) -> dict:
        """Create and store a validation error."""
        error_data = {
            'run_id': run_id,
            'path_execution_id': path_execution_id,
            'validation_test_id': self._get_validation_test_id(error_type),
            'severity': severity.value,
            'error_scope': self._get_error_scope(error_type),
            'error_type': error_type,
            'object_type': object_type,
            'object_id': object_id,
            'object_guid': '',  # Will be populated if available
            'object_is_loopback': False,
            'notes': notes
        }
        
        self._store_validation_error(error_data)
        return error_data
    
    def _create_review_flag(self, run_id: str, error: dict, path_execution: dict):
        """Create a review flag for critical errors."""
        flag_data = {
            'run_id': run_id,
            'flag_type': 'CRITICAL_ERROR',
            'severity': error['severity'],
            'reason': f'Critical validation error: {error["error_type"]}',
            'object_type': error['object_type'],
            'object_id': error['object_id'],
            'object_guid': error.get('object_guid', ''),
            'object_is_loopback': False,
            'path_context': path_execution.get('path_context', ''),
            'status': 'OPEN'
        }
        
        self._store_review_flag(flag_data)
    
    # Database methods
    def _fetch_run_path_executions(self, run_id: str) -> list[dict]:
        query = '''
            SELECT id, run_id, path_id, execution_status, coverage, cost, 
                   length_mm, node_count, link_count, utilities_scope, path_context
            FROM tb_path_executions 
            WHERE run_id = %s
        '''
        return self.db.fetch_all(query, (run_id,))
    
    def _fetch_nodes_data(self, node_ids: list) -> list[dict]:
        if not node_ids:
            return []
        
        placeholders = ', '.join(['%s'] * len(node_ids))
        query = f'''
            SELECT id, guid, fab_no, model_no, phase_no, utility_no, data_code
            FROM nw_nodes 
            WHERE id IN ({placeholders})
        '''
        return self.db.fetch_all(query, tuple(node_ids))
    
    def _fetch_links_data(self, link_ids: list) -> list[dict]:
        if not link_ids:
            return []
        
        placeholders = ', '.join(['%s'] * len(link_ids))
        query = f'''
            SELECT id, guid, start_node_id, end_node_id, fab_no, model_no, 
                   phase_no, utility_no, material_no, flow
            FROM nw_links 
            WHERE id IN ({placeholders})
        '''
        return self.db.fetch_all(query, tuple(link_ids))
    
    def _fetch_nodes_with_utilities(self, node_ids: list) -> list[dict]:
        if not node_ids:
            return []
            
        placeholders = ', '.join(['%s'] * len(node_ids))
        query = f'''
            SELECT id, utility_no, data_code
            FROM nw_nodes 
            WHERE id IN ({placeholders})
        '''
        return self.db.fetch_all(query, tuple(node_ids))
    
    def _fetch_links_with_utilities(self, link_ids: list) -> list[dict]:
        if not link_ids:
            return []
            
        placeholders = ', '.join(['%s'] * len(link_ids))
        query = f'''
            SELECT id, utility_no, material_no, flow
            FROM nw_links 
            WHERE id IN ({placeholders})
        '''
        return self.db.fetch_all(query, tuple(link_ids))
    
    def _load_validation_tests(self) -> dict:
        """Load validation tests from database."""
        query = '''
            SELECT code, name, scope, severity, test_type, reason, is_active
            FROM tb_validation_tests 
            WHERE is_active = 1
        '''
        tests = self.db.fetch_all(query)
        return {test['code']: test for test in tests}
    
    def _get_validation_test_id(self, error_type: str) -> int:
        """Get validation test ID for error type based on error category."""
        # Map error types to validation test codes
        error_to_test_map = {
            # Connectivity errors
            'NOT_FOUND_NODE': 'PATH_CONN_001',
            'NOT_FOUND_LINK': 'PATH_CONN_001', 
            'NOT_FOUND_NODES': 'PATH_CONN_001',
            'NOT_FOUND_LINKS': 'PATH_CONN_001',
            'MISSING_START_NODE': 'PATH_CONN_002',
            'MISSING_END_NODE': 'PATH_CONN_002',
            'MISSING_POC': 'PATH_CONN_002',
            'INVALID_NODE': 'PATH_NODE_001',
            'INVALID_LINK': 'PATH_LINK_001',
            'INVALID_S_NODE': 'PATH_NODE_001',
            'INVALID_E_NODE': 'PATH_NODE_001',
            'DISCONNECTED': 'PATH_CONN_003',
            'BROKEN_CONTINUITY': 'PATH_CONN_003',
            'DISCONNECTED_LINK': 'PATH_CONN_003',
            'CONNECTING_GAP': 'PATH_CONN_003',
            'MISSING_NWO_TYPE': 'PATH_CONN_004',
            
            # Utility errors
            'MISSING_UTILITY': 'PATH_UTY_001',
            'INVALID_UTILITY': 'PATH_UTY_001',
            'WRONG_UTILITY': 'PATH_UTY_001',
            'UTILITY_MISMATCH': 'PATH_UTY_002',
            'INVALID_TRANSITION': 'PATH_UTY_002',
            'FLOW_DIRECTION_ISSUE': 'PATH_UTY_003',
            'BIDIRECTIONAL_ISSUE': 'PATH_UTY_003',
            
            # Material errors
            'MISSING_MATERIAL': 'PATH_MAT_001',
            'INVALID_MATERIAL': 'PATH_MAT_001',
            'WRONG_MATERIAL': 'PATH_MAT_001',
            'MATERIAL_MISMATCH': 'PATH_MAT_002',
            'INCOMPATIBILITY': 'PATH_MAT_002',
            
            # Flow errors
            'MISSING_FLOW': 'PATH_FLOW_001',
            'INVALID_FLOW': 'PATH_FLOW_001',
            'WRONG_FLOW': 'PATH_FLOW_001',
            'FLOW_MISMATCH': 'PATH_FLOW_002',
            
            # Data quality errors
            'MISSING_GUID': 'PATH_QA_001',
            'MISSING_DATA_CODE': 'PATH_QA_001',
            'MISSING_MARKERS': 'PATH_QA_001',
            'MISSING_COST': 'PATH_QA_002',
            'NEGATIVE_COST': 'PATH_QA_002',
            'MISSING_REFERENCE': 'PATH_QA_003',
            'MISSING': 'PATH_QA_003',
            'MISSING_PATH_DATA': 'PATH_QA_004',
            
            # Performance errors
            'UNUSUAL_LENGTH': 'PATH_PERF_001',
            'INVALID_LENGTH': 'PATH_PERF_001',
            'HIGH_COMPLEXITY': 'PATH_PERF_001',
            'UNUSUAL_TOPOLOGY': 'PATH_PERF_002',
            'REDUNDANT_NODES': 'PATH_PERF_002',
            'CIRCULAR_LOOP_DETECTED': 'PATH_PERF_002',
            'NOT_USED': 'PATH_PERF_003',
            'NOT_USED_POC': 'PATH_PERF_003',
            'USAGE_STATUS': 'PATH_PERF_003',
            
            # Scenario errors
            'CROSS_CONTAMINATION': 'PATH_SCEN_001',
            
            # Generic errors
            'INVALID': 'PATH_GEN_001',
            'WRONG': 'PATH_GEN_002',
            'MISMATCH': 'PATH_GEN_002',
            'UNUSUAL': 'PATH_GEN_002',
            'INVALID_CHANGE': 'PATH_GEN_003'
        }
        
        test_code = error_to_test_map.get(error_type, 'PATH_GEN_001')  # Default to generic test
        
        query = 'SELECT id FROM tb_validation_tests WHERE code = %s'
        result = self.db.fetch_one(query, (test_code,))
        return result['id'] if result else 1  # Fallback to ID 1
    
    def _get_error_scope(self, error_type: str) -> str:
        """Determine error scope based on error type."""
        if 'CONNECTIVITY' in error_type or 'CONTINUITY' in error_type:
            return 'CONNECTIVITY'
        elif 'UTILITY' in error_type:
            return 'FLOW'
        elif 'MATERIAL' in error_type:
            return 'MATERIAL'
        else:
            return 'QA'
    
    def _store_validation_error(self, error_data: dict):
        """Store validation error in database."""
        query = '''
            INSERT INTO tb_validation_errors (
                run_id, path_execution_id, validation_test_id, severity,
                error_scope, error_type, object_type, object_id, object_guid,
                object_is_loopback, notes
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        '''
        params = (
            error_data['run_id'], error_data['path_execution_id'], 
            error_data['validation_test_id'], error_data['severity'],
            error_data['error_scope'], error_data['error_type'],
            error_data['object_type'], error_data['object_id'],
            error_data['object_guid'], error_data['object_is_loopback'],
            error_data.get('notes')
        )
        self.db.execute_update(query, params)
    
    def _store_path_tag(self, run_id: str, path_id: int, tag_type: str, tag_code: str, 
                       tag: str, source: str, confidence: float):
        """Store path tag for AI training."""
        query = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, tag_type, tag_code, tag, 
                source, confidence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        self.db.execute_update(query, (run_id, path_id, tag_type, tag_code, tag, source, confidence))
    
    def _store_review_flag(self, flag_data: dict):
        """Store review flag in database."""
        query = '''
            INSERT INTO tb_run_reviews (
                run_id, flag_type, severity, reason, object_type, object_id,
                object_guid, object_is_loopback, path_context, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        params = (
            flag_data['run_id'], flag_data['flag_type'], flag_data['severity'],
            flag_data['reason'], flag_data['object_type'], flag_data['object_id'],
            flag_data['object_guid'], flag_data['object_is_loopback'],
            flag_data['path_context'], flag_data['status']
        )
        self.db.execute_update(query, params)
    
    def _update_path_execution_validation_status(self, run_id: str, validation_results: dict):
        """Update path execution validation status."""
        total_errors = validation_results['total_errors']
        critical_errors = validation_results['critical_errors']
        
        # Update all path executions for the run
        update_query = '''
            UPDATE tb_path_executions 
            SET validation_passed = %s,
                validation_errors = %s
            WHERE run_id = %s
        '''
        
        validation_passed = 1 if critical_errors == 0 else 0
        validation_summary = json.dumps({
            'total_errors': total_errors,
            'critical_errors': critical_errors,
            'errors_by_severity': dict(validation_results['errors_by_severity'])
        })
        
        self.db.execute_update(update_query, (validation_passed, validation_summary, run_id))
    
    def _print_validation_summary(self, results: dict):
        """Print validation summary."""
        print('\n=== Validation Summary ===')
        print(f'Total paths validated: {results["total_paths_validated"]}')
        print(f'Total errors: {results["total_errors"]}')
        print(f'Critical errors: {results["critical_errors"]}')
        print(f'Review flags created: {results["total_review_flags"]}')
        
        if results['errors_by_severity']:
            print('\nErrors by severity:')
            for severity, count in results['errors_by_severity'].items():
                print(f'  {severity.value}: {count}')
        
        if results['errors_by_type']:
            print('\nTop error types:')
            top_errors = results['errors_by_type'].most_common(5)
            for error_type, count in top_errors:
                print(f'  {error_type}: {count}')
        
        print('========================\n')
