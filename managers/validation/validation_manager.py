from typing import Any, Optional, Counter
from dataclasses import dataclass, field
from collections import Counter
import time
import json

from .validation_enums import ValidationScope, Severity, ValidationType, ObjectType, ErrorType, TagType, TagCode
from .validation_models import ValidationResult, PathValidation, ValidationTest, ValidationError
from .database import Database
from .string_helper import StringHelper


class ValidationManager:
    """Comprehensive path validation framework."""
    
    def __init__(self, db: Database, verbose: bool = False, silent: bool = False):
        self.db = db
        self.verbose = verbose
        self.silent = silent
        self.validation_tests = self._load_validation_tests()
        
        # Cache for validation test lookups
        self._test_cache = {test.code: test for test in self.validation_tests}
    
    def validate_run_paths(self, run_id: str) -> dict[str, Any]:
        """
        Validate all paths in a run and return comprehensive results.
        Performs connectivity and utility validation on all paths.
        """
        if not self.silent:
            print(f'Starting validation for run {run_id}')
        
        start_time = time.time()
        
        # Get all path executions for the run
        paths = self._fetch_run_path_executions(run_id)
        
        if not paths:
            return {
                'total_paths_validated': 0,
                'total_errors': 0,
                'total_reviews': 0,
                'critical_errors': 0,
                'errors_by_severity': Counter(),
                'errors_by_type': Counter(),
                'validation_summary': {},
                'execution_time_s': time.time() - start_time
            }
        
        validation_results = {
            'total_paths_validated': len(paths),
            'total_errors': 0,
            'total_reviews': 0,
            'critical_errors': 0,
            'errors_by_severity': Counter(),
            'errors_by_type': Counter(),
            'validation_summary': {},
            'paths_with_errors': [],
            'paths_passed': 0
        }
        
        for path_execution in paths:
            path_result = self._validate_single_path(run_id, path_execution)
            
            if path_result:
                # Update path execution with validation results
                self._update_path_execution_validation(path_execution, path_result)
                
                # Generate and store path tags for AI training
                tags = self._generate_path_tags(run_id, path_execution, path_result)
                self._store_path_tags(run_id, path_execution.path_id, tags)
                
                # Aggregate results
                validation_results['total_errors'] += len(path_result)
                validation_results['paths_with_errors'].append(path_execution.path_id)
                
                for error in path_result:
                    validation_results['errors_by_severity'][error.severity.value] += 1
                    validation_results['errors_by_type'][error.error_type.value] += 1
                    
                    if error.severity == Severity.CRITICAL:
                        validation_results['critical_errors'] += 1
                
                # Create review flags for critical issues
                self._create_review_flags(run_id, path_execution, path_result)
            else:
                validation_results['paths_passed'] += 1
                # Generate tags for passed paths too
                tags = self._generate_path_tags(run_id, path_execution, [])
                self._store_path_tags(run_id, path_execution.path_id, tags)
        
        validation_results['execution_time_s'] = time.time() - start_time
        validation_results['validation_summary'] = self._create_validation_summary(validation_results)
        
        if not self.silent:
            print(f'Validation completed in {validation_results["execution_time_s"]:.2f}s')
            print(f'Paths validated: {validation_results["total_paths_validated"]}')
            print(f'Total errors: {validation_results["total_errors"]}')
            print(f'Critical errors: {validation_results["critical_errors"]}')
        
        return validation_results
    
    def _validate_single_path(
        self,
        run_id: str,
        path: PathValidation
    ) -> Optional[list[ValidationError]]:
        """Validate a single path and return any errors found."""
        if not run_id or not path:
            return None
            
        if self.verbose:
            print(f'Validating path {path.path_id}')
        
        errors = []
        
        # Run connectivity validation tests
        errors.extend(self._validate_connectivity(run_id, path))
        
        # Run utility consistency validation tests  
        errors.extend(self._validate_utility_consistency(run_id, path))
        
        # Run PoC configuration validation
        errors.extend(self._validate_poc_configuration(run_id, path))
        
        # Run structural validation tests
        errors.extend(self._validate_path_structure(run_id, path))
        
        # Run performance validation tests
        errors.extend(self._validate_path_performance(run_id, path))
        
        # Store all errors in database
        if errors:
            self._store_validation_errors(errors)
        
        return errors if errors else None
    
    def _validate_connectivity(self, run_id: str, path: PathValidation) -> list[ValidationError]:
        """Validate path connectivity - most critical validation."""
        errors = []
        
        # PATH_CONN_001: Path Data Completeness
        if not path.node_ids or not path.network:
            if not path.node_ids:
                errors.append(self._create_error(
                    run_id, path, 'PATH_CONN_001', Severity.CRITICAL,
                    ErrorType.NOT_FOUND_NODES, ObjectType.PATH, path.path_id,
                    'Path contains no nodes'
                ))
            if not path.network:
                errors.append(self._create_error(
                    run_id, path, 'PATH_CONN_001', Severity.CRITICAL,
                    ErrorType.NOT_FOUND_LINKS, ObjectType.PATH, path.path_id,
                    'Path contains no links'
                ))
        
        # PATH_CONN_002: Node Existence Validation
        valid_nodes = self._fetch_valid_node_ids(path.node_ids)
        invalid_nodes = path.node_ids - valid_nodes
        for node_id in invalid_nodes:
            errors.append(self._create_error(
                run_id, path, 'PATH_CONN_002', Severity.CRITICAL,
                ErrorType.INVALID_NODE, ObjectType.NODE, node_id,
                f'Node {node_id} not found in sampling universe'
            ))
        
        # PATH_CONN_003: Link Existence Validation  
        link_ids = {link.link_id for link in path.network}
        valid_links = self._fetch_valid_link_ids(link_ids)
        invalid_links = link_ids - valid_links
        for link_id in invalid_links:
            errors.append(self._create_error(
                run_id, path, 'PATH_CONN_003', Severity.CRITICAL,
                ErrorType.INVALID_LINK, ObjectType.LINK, link_id,
                f'Link {link_id} not found in sampling universe'
            ))
        
        # PATH_CONN_004: Path Endpoints Validation
        if not path.s_node_id or not path.e_node_id:
            if not path.s_node_id:
                errors.append(self._create_error(
                    run_id, path, 'PATH_CONN_004', Severity.CRITICAL,
                    ErrorType.MISSING_START_NODE, ObjectType.PATH, path.path_id,
                    'Path missing start node'
                ))
            if not path.e_node_id:
                errors.append(self._create_error(
                    run_id, path, 'PATH_CONN_004', Severity.CRITICAL,
                    ErrorType.MISSING_END_NODE, ObjectType.PATH, path.path_id,
                    'Path missing end node'
                ))
        
        # PATH_CONN_005: Node-Link Continuity
        continuity_errors = self._check_path_continuity(path)
        for error_data in continuity_errors:
            errors.append(self._create_error(
                run_id, path, 'PATH_CONN_005', Severity.CRITICAL,
                ErrorType.BROKEN_CONTINUITY, ObjectType.LINK, error_data['link_id'],
                f'Continuity broken at link {error_data["link_id"]}'
            ))
        
        # PATH_CONN_006: Disconnected Elements
        disconnected = self._find_disconnected_elements(path)
        for element in disconnected:
            errors.append(self._create_error(
                run_id, path, 'PATH_CONN_006', Severity.ERROR,
                ErrorType.DISCONNECTED, ObjectType.NODE if element['type'] == 'node' else ObjectType.LINK,
                element['id'], f'Disconnected {element["type"]} {element["id"]}'
            ))
        
        # PATH_CONN_007: Bidirectional Consistency
        bidirectional_issues = self._check_bidirectional_consistency(path)
        for issue in bidirectional_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_CONN_007', Severity.ERROR,
                ErrorType.BIDIRECTIONAL_ISSUE, ObjectType.LINK, issue['link_id'],
                f'Bidirectional inconsistency in link {issue["link_id"]}'
            ))
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path: PathValidation) -> list[ValidationError]:
        """Validate utility consistency along the path."""
        errors = []
        
        # PATH_UTY_001: Utility Code Presence
        nodes_without_utility = self._find_nodes_missing_utility(path)
        for node_data in nodes_without_utility:
            errors.append(self._create_error(
                run_id, path, 'PATH_UTY_001', Severity.ERROR,
                ErrorType.MISSING_UTILITY, ObjectType.NODE, node_data['node_id'],
                f'Node {node_data["node_id"]} missing utility code',
                object_utility_no=node_data.get('utility_no')
            ))
        
        # PATH_UTY_002: Utility Consistency
        utility_mismatches = self._find_utility_mismatches(path)
        for mismatch in utility_mismatches:
            errors.append(self._create_error(
                run_id, path, 'PATH_UTY_002', Severity.ERROR,
                ErrorType.UTILITY_MISMATCH, ObjectType.LINK, mismatch['link_id'],
                f'Utility mismatch between nodes {mismatch["s_node_id"]} and {mismatch["e_node_id"]}'
            ))
        
        # PATH_UTY_003: Utility Transitions
        invalid_transitions = self._find_invalid_utility_transitions(path)
        for transition in invalid_transitions:
            errors.append(self._create_error(
                run_id, path, 'PATH_UTY_003', Severity.WARNING,
                ErrorType.INVALID_TRANSITION, ObjectType.LINK, transition['link_id'],
                f'Invalid utility transition from {transition["from_utility"]} to {transition["to_utility"]}'
            ))
        
        # PATH_UTY_004: Utility Scope Compliance
        if path.utilities_scope:
            scope_violations = self._find_utility_scope_violations(path)
            for violation in scope_violations:
                errors.append(self._create_error(
                    run_id, path, 'PATH_UTY_004', Severity.WARNING,
                    ErrorType.WRONG_UTILITY, ObjectType.NODE, violation['node_id'],
                    f'Node {violation["node_id"]} utility {violation["utility"]} not in scope'
                ))
        
        return errors
    
    def _validate_poc_configuration(self, run_id: str, path: PathValidation) -> list[ValidationError]:
        """Validate PoC configuration compliance."""
        errors = []
        
        # PATH_POC_001: PoC Reference Validation
        if path.references_scope:
            invalid_refs = self._find_invalid_poc_references(path)
            for ref in invalid_refs:
                errors.append(self._create_error(
                    run_id, path, 'PATH_POC_001', Severity.WARNING,
                    ErrorType.MISSING_REFERENCE, ObjectType.POC, ref['poc_id'],
                    f'Invalid PoC reference {ref["poc_id"]}'
                ))
        
        # PATH_POC_002: PoC Usage Status
        unused_pocs = self._find_unused_poc_configurations(path)
        for poc in unused_pocs:
            errors.append(self._create_error(
                run_id, path, 'PATH_POC_002', Severity.WARNING,
                ErrorType.NOT_USED_POC, ObjectType.POC, poc['poc_id'],
                f'PoC configuration {poc["poc_id"]} defined but not used'
            ))
        
        return errors
    
    def _validate_path_structure(self, run_id: str, path: PathValidation) -> list[ValidationError]:
        """Validate path structural integrity."""
        errors = []
        
        # PATH_STR_001: Path Length Validation
        if path.length_mm < 0:
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_001', Severity.WARNING,
                ErrorType.INVALID_LENGTH, ObjectType.PATH, path.path_id,
                f'Negative path length: {path.length_mm}mm'
            ))
        elif path.length_mm > 1000000:  # 1km threshold for unusual length
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_001', Severity.WARNING,
                ErrorType.UNUSUAL_LENGTH, ObjectType.PATH, path.path_id,
                f'Unusually long path: {path.length_mm}mm'
            ))
        
        # PATH_STR_002: Node Redundancy Check
        redundant_nodes = self._find_redundant_nodes(path)
        for node_id in redundant_nodes:
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_002', Severity.WARNING,
                ErrorType.REDUNDANT_NODES, ObjectType.NODE, node_id,
                f'Potentially redundant node {node_id}'
            ))
        
        # PATH_STR_003: Circular Loop Detection
        circular_loops = self._detect_circular_loops(path)
        for loop in circular_loops:
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_003', Severity.ERROR,
                ErrorType.CIRCULAR_LOOP_DETECTED, ObjectType.PATH, path.path_id,
                f'Circular loop detected involving nodes: {", ".join(map(str, loop["nodes"]))}'
            ))
        
        # PATH_STR_004: Path Complexity Analysis
        complexity_score = self._calculate_path_complexity(path)
        if complexity_score > 10:  # High complexity threshold
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_004', Severity.INFO,
                ErrorType.HIGH_COMPLEXITY, ObjectType.PATH, path.path_id,
                f'High path complexity score: {complexity_score}'
            ))
        
        # PATH_STR_005: Virtual Node Validation
        virtual_node_issues = self._validate_virtual_nodes(path)
        for issue in virtual_node_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_STR_005', Severity.INFO,
                ErrorType.MISSING_GUID, ObjectType.NODE, issue['node_id'],
                f'Virtual node {issue["node_id"]} missing expected data: {issue["missing_data"]}'
            ))
        
        return errors
    
    def _validate_path_performance(self, run_id: str, path: PathValidation) -> list[ValidationError]:
        """Validate path performance characteristics."""
        errors = []
        
        # PATH_PER_001: Cost Validation
        if path.const < 0:
            errors.append(self._create_error(
                run_id, path, 'PATH_PER_001', Severity.WARNING,
                ErrorType.NEGATIVE_COST, ObjectType.PATH, path.path_id,
                f'Negative path cost: {path.const}',
                object_cost=path.const
            ))
        
        # PATH_PER_002: Material Consistency
        material_issues = self._check_material_consistency(path)
        for issue in material_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_PER_002', Severity.WARNING,
                ErrorType.MATERIAL_MISMATCH, ObjectType.LINK, issue['link_id'],
                f'Material inconsistency at link {issue["link_id"]}'
            ))
        
        # PATH_PER_003: Flow Direction Analysis
        flow_issues = self._analyze_flow_directions(path)
        for issue in flow_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_PER_003', Severity.WARNING,
                ErrorType.FLOW_DIRECTION_ISSUE, ObjectType.LINK, issue['link_id'],
                f'Flow direction issue at link {issue["link_id"]}'
            ))
        
        # PATH_PER_004: Data Code Validation
        data_code_issues = self._validate_data_codes(path)
        for issue in data_code_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_PER_004', Severity.INFO,
                ErrorType.MISSING_DATA_CODE, ObjectType.NODE, issue['node_id'],
                f'Missing or invalid data code for node {issue["node_id"]}'
            ))
        
        # PATH_PER_005: Path Markers Analysis
        marker_issues = self._analyze_path_markers(path)
        for issue in marker_issues:
            errors.append(self._create_error(
                run_id, path, 'PATH_PER_005', Severity.INFO,
                ErrorType.MISSING_MARKERS, ObjectType.NODE, issue['node_id'],
                f'Missing path markers for node {issue["node_id"]}'
            ))
        
        return errors
    
    def _create_error(
        self,
        run_id: str,
        path: PathValidation,
        test_code: str,
        severity: Severity,
        error_type: ErrorType,
        object_type: ObjectType,
        object_id: int,
        message: str,
        **kwargs
    ) -> ValidationError:
        """Create a validation error with all required fields."""
        test = self._test_cache.get(test_code)
        if not test:
            raise ValueError(f'Unknown validation test code: {test_code}')
        
        return ValidationError(
            run_id=run_id,
            path_execution_id=path.execution_id,
            validation_test_id=test.id,
            severity=severity,
            error_scope=test.scope,
            error_type=error_type,
            object_type=object_type.value,
            object_id=object_id,
            object_guid=kwargs.get('object_guid', ''),
            error_message=message,
            object_fab_no=kwargs.get('object_fab_no'),
            object_model_no=kwargs.get('object_model_no'),
            object_data_code=kwargs.get('object_data_code'),
            object_e2e_group_no=kwargs.get('object_e2e_group_no'),
            object_markers=kwargs.get('object_markers'),
            object_utility_no=kwargs.get('object_utility_no'),
            object_item_no=kwargs.get('object_item_no'),
            object_type_no=kwargs.get('object_type_no'),
            object_material_no=kwargs.get('object_material_no'),
            object_flow=kwargs.get('object_flow'),
            object_is_loopback=kwargs.get('object_is_loopback', False),
            object_cost=kwargs.get('object_cost'),
            error_data=kwargs.get('error_data'),
            notes=kwargs.get('notes')
        )
    
    def _generate_path_tags(
        self,
        run_id: str,
        path: PathValidation,
        errors: list[ValidationError]
    ) -> list[dict]:
        """Generate AI training tags based on path validation results."""
        tags = []
        
        # Basic connectivity tag
        if any(e.error_type in [ErrorType.NOT_FOUND_NODES, ErrorType.NOT_FOUND_LINKS, 
                               ErrorType.BROKEN_CONTINUITY] for e in errors):
            tags.append({
                'tag_type': TagType.CRIT.value,
                'tag_code': TagCode.CONN_BROKEN.value,
                'tag': 'Critical connectivity issues',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        elif any(e.error_scope == ValidationScope.CONNECTIVITY for e in errors):
            tags.append({
                'tag_type': TagType.QA.value,
                'tag_code': TagCode.CONN_PARTIAL.value,
                'tag': 'Connectivity issues detected',
                'source': 'VALIDATION',
                'confidence': 0.8
            })
        else:
            tags.append({
                'tag_type': TagType.QA.value,
                'tag_code': TagCode.CONN_VALID.value,
                'tag': 'Connectivity validated',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        
        # Utility consistency tags
        utility_errors = [e for e in errors if e.error_scope == ValidationScope.UTILITY]
        if utility_errors:
            if len(utility_errors) > 3:
                tags.append({
                    'tag_type': TagType.UTY.value,
                    'tag_code': TagCode.UTY_MIXED.value,
                    'tag': 'Multiple utility issues',
                    'source': 'VALIDATION',
                    'confidence': 0.9
                })
            else:
                tags.append({
                    'tag_type': TagType.UTY.value,
                    'tag_code': TagCode.UTY_TRANSITION.value,
                    'tag': 'Utility transition issues',
                    'source': 'VALIDATION',
                    'confidence': 0.7
                })
        else:
            tags.append({
                'tag_type': TagType.UTY.value,
                'tag_code': TagCode.UTY_CONSISTENT.value,
                'tag': 'Utility consistency validated',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        
        # Risk assessment tags
        critical_errors = [e for e in errors if e.severity == Severity.CRITICAL]
        if critical_errors:
            tags.append({
                'tag_type': TagType.RISK.value,
                'tag_code': TagCode.RISK_HIGH.value,
                'tag': f'{len(critical_errors)} critical issues',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        elif len(errors) > 5:
            tags.append({
                'tag_type': TagType.RISK.value,
                'tag_code': TagCode.RISK_MEDIUM.value,
                'tag': 'Multiple validation issues',
                'source': 'VALIDATION',
                'confidence': 0.8
            })
        else:
            tags.append({
                'tag_type': TagType.RISK.value,
                'tag_code': TagCode.RISK_LOW.value,
                'tag': 'Low risk path',
                'source': 'VALIDATION',
                'confidence': 0.9
            })
        
        # Performance tags
        if path.const > 1000:  # High cost threshold
            tags.append({
                'tag_type': TagType.CAT.value,
                'tag_code': TagCode.PERF_EXPENSIVE.value,
                'tag': 'High cost path',
                'source': 'VALIDATION',
                'confidence': 0.8
            })
        elif path.length_mm > 100000:  # Long path threshold
            tags.append({
                'tag_type': TagType.CAT.value,
                'tag_code': TagCode.PERF_SLOW.value,
                'tag': 'Long path length',
                'source': 'VALIDATION',
                'confidence': 0.7
            })
        else:
            tags.append({
                'tag_type': TagType.CAT.value,
                'tag_code': TagCode.PERF_OPTIMAL.value,
                'tag': 'Optimal performance',
                'source': 'VALIDATION',
                'confidence': 0.6
            })
        
        # Structural complexity tags
        complexity = self._calculate_path_complexity(path)
        if complexity > 10:
            tags.append({
                'tag_type': TagType.CAT.value,
                'tag_code': TagCode.STRUCT_COMPLEX.value,
                'tag': 'Complex path structure',
                'source': 'VALIDATION',
                'confidence': 0.8
            })
        elif complexity < 3:
            tags.append({
                'tag_type': TagType.CAT.value,
                'tag_code': TagCode.STRUCT_SIMPLE.value,
                'tag': 'Simple path structure',
                'source': 'VALIDATION',
                'confidence': 0.9
            })
        
        # Virtual node tags
        virtual_nodes = self._count_virtual_nodes(path)
        if virtual_nodes > 0:
            tags.append({
                'tag_type': TagType.DAT.value,
                'tag_code': TagCode.STRUCT_VIRTUAL.value,
                'tag': f'{virtual_nodes} virtual nodes',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        
        # Overall quality tag
        if not errors:
            tags.append({
                'tag_type': TagType.QA.value,
                'tag_code': TagCode.QA_PASSED.value,
                'tag': 'All validations passed',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        elif any(e.severity == Severity.CRITICAL for e in errors):
            tags.append({
                'tag_type': TagType.QA.value,
                'tag_code': TagCode.QA_FAILED.value,
                'tag': 'Critical validation failures',
                'source': 'VALIDATION',
                'confidence': 1.0
            })
        elif len(errors) > 3:
            tags.append({
                'tag_type': TagType.QA.value,
                'tag_code': TagCode.QA_REVIEW.value,
                'tag': 'Multiple issues require review',
                'source': 'VALIDATION',
                'confidence': 0.8
            })
        
        return tags
    
    # Helper methods for specific validation checks
    def _check_path_continuity(self, path: PathValidation) -> list[dict]:
        """Check if path links create continuous connectivity."""
        continuity_errors = []
        
        if not path.network:
            return continuity_errors
        
        # Sort links by sequence
        sorted_links = sorted(path.network, key=lambda x: x.seq)
        
        for i in range(len(sorted_links) - 1):
            current_link = sorted_links[i]
            next_link = sorted_links[i + 1]
            
            # Check if current link's end node connects to next link's start node
            current_end = current_link.e_node_id
            next_start = next_link.s_node_id
            
            if current_end != next_start:
                continuity_errors.append({
                    'link_id': next_link.link_id,
                    'expected_start': current_end,
                    'actual_start': next_start
                })
        
        return continuity_errors
    
    def _find_disconnected_elements(self, path: PathValidation) -> list[dict]:
        """Find disconnected nodes or links in the path."""
        disconnected = []
        
        # Get all nodes that should be connected by links
        connected_nodes = set()
        for link in path.network:
            connected_nodes.add(link.s_node_id)
            connected_nodes.add(link.e_node_id)
        
        # Find nodes in path that are not connected
        disconnected_nodes = path.node_ids - connected_nodes
        for node_id in disconnected_nodes:
            disconnected.append({'type': 'node', 'id': node_id})
        
        return disconnected
    
    def _check_bidirectional_consistency(self, path: PathValidation) -> list[dict]:
        """Check bidirectional link consistency."""
        issues = []
        
        # Group links by their node pairs
        link_pairs = {}
        for link in path.network:
            key = tuple(sorted([link.s_node_id, link.e_node_id]))
            if key not in link_pairs:
                link_pairs[key] = []
            link_pairs[key].append(link)
        
        # Check for bidirectional issues
        for node_pair, links in link_pairs.items():
            if len(links) == 2:  # Should be bidirectional
                # Check if both directions are properly represented
                forward = next((l for l in links if not l.is_reverse), None)
                reverse = next((l for l in links if l.is_reverse), None)
                
                if not forward or not reverse:
                    issues.append({
                        'link_id': links[0].link_id,
                        'issue': 'Missing bidirectional pair'
                    })
        
        return issues
    
    def _find_nodes_missing_utility(self, path: PathValidation) -> list[dict]:
        """Find nodes that should have utility codes but don't."""
        missing_utility = []
        
        # Get node details to check for virtual nodes
        node_details = self._fetch_node_details(list(path.node_ids))
        
        for node in node_details:
            # Skip virtual nodes (nwo_type != 101)
            if node.get('nwo_type') == 101 and not node.get('utility_no'):
                missing_utility.append({
                    'node_id': node['id'],
                    'utility_no': node.get('utility_no')
                })
        
        return missing_utility
    
    def _find_utility_mismatches(self, path: PathValidation) -> list[dict]:
        """Find utility mismatches between connected nodes."""
        mismatches = []
        
        for link in path.network:
            if (link.s_node_utility_no and link.e_node_utility_no and 
                link.s_node_utility_no != link.e_node_utility_no):
                mismatches.append({
                    'link_id': link.link_id,
                    's_node_id': link.s_node_id,
                    'e_node_id': link.e_node_id,
                    's_utility': link.s_node_utility_no,
                    'e_utility': link.e_node_utility_no
                })
        
        return mismatches
    
    def _find_invalid_utility_transitions(self, path: PathValidation) -> list[dict]:
        """Find invalid utility transitions along the path."""
        invalid_transitions = []
        
        # Define allowed utility transitions (this could be configurable)
        allowed_transitions = self._get_allowed_utility_transitions()
        
        for link in path.network:
            if (link.s_node_utility_no and link.e_node_utility_no and
                link.s_node_utility_no != link.e_node_utility_no):
                
                transition = (link.s_node_utility_no, link.e_node_utility_no)
                if transition not in allowed_transitions:
                    invalid_transitions.append({
                        'link_id': link.link_id,
                        'from_utility': link.s_node_utility_no,
                        'to_utility': link.e_node_utility_no
                    })
        
        return invalid_transitions
    
    def _find_utility_scope_violations(self, path: PathValidation) -> list[dict]:
        """Find nodes with utilities outside the defined scope."""
        violations = []
        
        if not path.utilities_scope:
            return violations
        
        for link in path.network:
            # Check start node utility
            if (link.s_node_utility_no and 
                link.s_node_utility_no not in path.utilities_scope):
                violations.append({
                    'node_id': link.s_node_id,
                    'utility': link.s_node_utility_no
                })
            
            # Check end node utility
            if (link.e_node_utility_no and 
                link.e_node_utility_no not in path.utilities_scope):
                violations.append({
                    'node_id': link.e_node_id,
                    'utility': link.e_node_utility_no
                })
        
        # Remove duplicates
        seen = set()
        unique_violations = []
        for violation in violations:
            key = (violation['node_id'], violation['utility'])
            if key not in seen:
                seen.add(key)
                unique_violations.append(violation)
        
        return unique_violations
    
    def _find_invalid_poc_references(self, path: PathValidation) -> list[dict]:
        """Find invalid PoC references in the path."""
        invalid_refs = []
        
        if not path.references_scope:
            return invalid_refs
        
        # This would need implementation based on how PoC references are stored
        # For now, return empty list as structure is not fully defined
        return invalid_refs
    
    def _find_unused_poc_configurations(self, path: PathValidation) -> list[dict]:
        """Find PoC configurations that are defined but not used."""
        unused_pocs = []
        
        if not path.references_scope:
            return unused_pocs
        
        # This would need implementation based on how PoC configurations are tracked
        # For now, return empty list as structure is not fully defined
        return unused_pocs
    
    def _find_redundant_nodes(self, path: PathValidation) -> list[int]:
        """Find potentially redundant nodes in the path."""
        redundant_nodes = []
        
        # Count how many links each node participates in
        node_usage = {}
        for link in path.network:
            node_usage[link.s_node_id] = node_usage.get(link.s_node_id, 0) + 1
            node_usage[link.e_node_id] = node_usage.get(link.e_node_id, 0) + 1
        
        # Nodes used only once (except start/end) might be redundant
        for node_id, usage_count in node_usage.items():
            if (usage_count == 1 and 
                node_id != path.s_node_id and 
                node_id != path.e_node_id):
                redundant_nodes.append(node_id)
        
        return redundant_nodes
    
    def _detect_circular_loops(self, path: PathValidation) -> list[dict]:
        """Detect circular loops in the path."""
        loops = []
        
        # Build adjacency list
        adjacency = {}
        for link in path.network:
            if link.s_node_id not in adjacency:
                adjacency[link.s_node_id] = []
            adjacency[link.s_node_id].append(link.e_node_id)
        
        # Use DFS to detect cycles
        visited = set()
        rec_stack = set()
        
        def dfs(node, path_nodes):
            if node in rec_stack:
                # Found a cycle
                cycle_start = path_nodes.index(node)
                cycle_nodes = path_nodes[cycle_start:]
                loops.append({'nodes': cycle_nodes})
                return True
            
            if node in visited:
                return False
            
            visited.add(node)
            rec_stack.add(node)
            path_nodes.append(node)
            
            for neighbor in adjacency.get(node, []):
                if dfs(neighbor, path_nodes.copy()):
                    return True
            
            rec_stack.remove(node)
            return False
        
        # Start DFS from path start node
        if path.s_node_id:
            dfs(path.s_node_id, [])
        
        return loops
    
    def _calculate_path_complexity(self, path: PathValidation) -> float:
        """Calculate path complexity score."""
        if not path.network:
            return 0.0
        
        # Base complexity from node and link counts
        complexity = len(path.node_ids) * 0.5 + len(path.network) * 0.3
        
        # Add complexity for utility changes
        utility_changes = 0
        for link in path.network:
            if (link.s_node_utility_no and link.e_node_utility_no and
                link.s_node_utility_no != link.e_node_utility_no):
                utility_changes += 1
        
        complexity += utility_changes * 0.8
        
        # Add complexity for path length relative to direct distance
        if path.length_mm > 0:
            # This is a simplified complexity calculation
            length_complexity = min(path.length_mm / 10000, 5.0)  # Cap at 5
            complexity += length_complexity
        
        return round(complexity, 2)
    
    def _validate_virtual_nodes(self, path: PathValidation) -> list[dict]:
        """Validate virtual node configurations."""
        issues = []
        
        # Get detailed node information
        node_details = self._fetch_node_details(list(path.node_ids))
        
        for node in node_details:
            if node.get('nwo_type') != 101:  # Virtual node
                missing_data = []
                
                # Virtual nodes might not have all data, but check for critical missing items
                if not node.get('guid'):
                    missing_data.append('guid')
                
                if missing_data:
                    issues.append({
                        'node_id': node['id'],
                        'missing_data': ', '.join(missing_data)
                    })
        
        return issues
    
    def _check_material_consistency(self, path: PathValidation) -> list[dict]:
        """Check material consistency along the path."""
        issues = []
        
        # This would require material data from links/nodes
        # Implementation depends on how material data is stored
        # For now, return empty list as material schema is not fully defined
        return issues
    
    def _analyze_flow_directions(self, path: PathValidation) -> list[dict]:
        """Analyze flow direction consistency."""
        issues = []
        
        # This would require flow direction data from links
        # Implementation depends on how flow data is stored
        # For now, return empty list as flow schema is not fully defined
        return issues
    
    def _validate_data_codes(self, path: PathValidation) -> list[dict]:
        """Validate data codes on path nodes."""
        issues = []
        
        for link in path.network:
            # Check if non-virtual nodes have data codes
            if not link.s_node_data_code:
                issues.append({
                    'node_id': link.s_node_id,
                    'issue': 'missing_data_code'
                })
            
            if not link.e_node_data_code:
                issues.append({
                    'node_id': link.e_node_id,
                    'issue': 'missing_data_code'
                })
        
        # Remove duplicates
        seen = set()
        unique_issues = []
        for issue in issues:
            if issue['node_id'] not in seen:
                seen.add(issue['node_id'])
                unique_issues.append(issue)
        
        return unique_issues
    
    def _analyze_path_markers(self, path: PathValidation) -> list[dict]:
        """Analyze path markers for completeness."""
        issues = []
        
        # This would require marker data from nodes/links
        # Implementation depends on how marker data is stored
        # For now, return empty list as marker schema is not fully defined
        return issues
    
    def _count_virtual_nodes(self, path: PathValidation) -> int:
        """Count virtual nodes in the path."""
        if not path.node_ids:
            return 0
        
        node_details = self._fetch_node_details(list(path.node_ids))
        virtual_count = sum(1 for node in node_details if node.get('nwo_type') != 101)
        
        return virtual_count
    
    def _get_allowed_utility_transitions(self) -> set:
        """Get allowed utility transitions from configuration."""
        # This would typically come from a configuration table
        # For now, return a basic set of allowed transitions
        return {
            (1, 2), (2, 1),  # Basic utility transitions
            (1, 3), (3, 1),
            (2, 3), (3, 2),
            # Add more as needed based on fab requirements
        }
    
    # Database access methods
    def _load_validation_tests(self) -> list[ValidationTest]:
        """Load active validation tests from database."""
        sql = '''
            SELECT id, code, name, scope, severity, test_type, reason, is_active, description
            FROM tb_validation_tests 
            WHERE is_active = 1
            ORDER BY code
        '''
        
        rows = self.db.fetch_all(sql)
        tests = []
        
        for row in rows:
            tests.append(ValidationTest(
                code=row['code'],
                name=row['name'],
                scope=ValidationScope(row['scope']),
                severity=Severity(row['severity']),
                test_type=ValidationType(row['test_type']),
                reason=row['reason'],
                is_active=bool(row['is_active']),
                description=row['description']
            ))
            # Add id to test object for error creation
            tests[-1].id = row['id']
        
        return tests
    
    def _fetch_run_path_executions(self, run_id: str) -> list[PathValidation]:
        """Fetch all path executions for a run."""
        sql = '''
            SELECT id, path_id, execution_status, node_count, link_count,
                   coverage, cost, length_mm, data_codes_scope, utilities_scope,
                   references_scope, path_context
            FROM tb_path_executions 
            WHERE run_id = ? AND execution_status = 'COMPLETED'
            ORDER BY path_id
        '''
        
        rows = self.db.fetch_all(sql, (run_id,))
        path_executions = []
        
        for row in rows:
            # Parse JSON fields
            data_codes = json.loads(row['data_codes_scope']) if row['data_codes_scope'] else None
            utilities = json.loads(row['utilities_scope']) if row['utilities_scope'] else None
            references = json.loads(row['references_scope']) if row['references_scope'] else None
            
            path_validation = PathValidation(
                run_id=run_id,
                path_id=row['path_id'],
                execution_id=row['id'],
                s_node_id=0,  # Will be populated from path context
                e_node_id=0,  # Will be populated from path context
                node_count=row['node_count'],
                link_count=row['link_count'],
                data_codes_scope=data_codes,
                utilities_scope=utilities,
                references_scope=references,
                const=row['cost'],
                length_mm=row['length_mm']
            )
            
            # Load path network data
            self._load_path_network_data(path_validation, row['path_context'])
            
            path_executions.append(path_validation)
        
        return path_executions
    
    def _load_path_network_data(self, path: PathValidation, path_context: str):
        """Load path network data from path context or database."""
        if not path_context:
            return
        
        try:
            context_data = json.loads(path_context)
            
            # Extract start and end nodes
            if 'start_node' in context_data:
                path.s_node_id = context_data['start_node']
            if 'end_node' in context_data:
                path.e_node_id = context_data['end_node']
            
            # Load path links and nodes
            path_links = self._fetch_path_links(path.path_id)
            path.network = path_links
            
            # Extract node IDs
            path.node_ids = set()
            for link in path_links:
                path.node_ids.add(link.s_node_id)
                path.node_ids.add(link.e_node_id)
                
        except (json.JSONDecodeError, KeyError) as e:
            if self.verbose:
                print(f'Warning: Could not parse path context for path {path.path_id}: {e}')
    
    def _fetch_path_links(self, path_id: int) -> list:
        """Fetch path links from database."""
        # This query would depend on your path link storage structure
        # Assuming you have a table that stores path links
        sql = '''
            SELECT path_id, seq, link_id, length, s_node_id, s_node_data_code, 
                   s_node_utility_no, e_node_id, e_node_data_code, e_node_utility_no, 
                   is_reverse, node_flag
            FROM tb_path_links 
            WHERE path_id = ?
            ORDER BY seq
        '''
        
        rows = self.db.fetch_all(sql, (path_id,))
        path_links = []
        
        for row in rows:
            from .validation_models import PathLink
            path_links.append(PathLink(
                path_id=row['path_id'],
                seq=row['seq'],
                link_id=row['link_id'],
                length=row['length'],
                s_node_id=row['s_node_id'],
                s_node_data_code=row['s_node_data_code'],
                s_node_utility_no=row['s_node_utility_no'],
                e_node_id=row['e_node_id'],
                e_node_data_code=row['e_node_data_code'],
                e_node_utility_no=row['e_node_utility_no'],
                is_reverse=bool(row['is_reverse']),
                node_flag=row['node_flag']
            ))
        
        return path_links
    
    def _fetch_valid_node_ids(self, node_ids: set[int]) -> set[int]:
        """Fetch valid node IDs from the sampling universe."""
        if not node_ids:
            return set()
        
        placeholders = ','.join('?' * len(node_ids))
        sql = f'''
            SELECT id FROM tb_nodes 
            WHERE id IN ({placeholders})
        '''
        
        rows = self.db.fetch_all(sql, tuple(node_ids))
        return {row['id'] for row in rows}
    
    def _fetch_valid_link_ids(self, link_ids: set[int]) -> set[int]:
        """Fetch valid link IDs from the sampling universe."""
        if not link_ids:
            return set()
        
        placeholders = ','.join('?' * len(link_ids))
        sql = f'''
            SELECT id FROM tb_links 
            WHERE id IN ({placeholders})
        '''
        
        rows = self.db.fetch_all(sql, tuple(link_ids))
        return {row['id'] for row in rows}
    
    def _fetch_node_details(self, node_ids: list[int]) -> list[dict]:
        """Fetch detailed node information."""
        if not node_ids:
            return []
        
        placeholders = ','.join('?' * len(node_ids))
        sql = f'''
            SELECT id, guid, nwo_type, utility_no, data_code, material_no
            FROM tb_nodes 
            WHERE id IN ({placeholders})
        '''
        
        rows = self.db.fetch_all(sql, tuple(node_ids))
        return [dict(row) for row in rows]
    
    def _store_validation_errors(self, errors: list[ValidationError]):
        """Store validation errors in database."""
        if not errors:
            return
        
        sql = '''
            INSERT INTO tb_validation_errors (
                run_id, path_execution_id, validation_test_id, severity, error_scope,
                error_type, object_type, object_id, object_guid, object_fab_no,
                object_model_no, object_phase_no, object_data_code, object_e2e_group_no,
                object_markers, object_utility_no, object_item_no, object_type_no,
                object_material_no, object_flow, object_is_loopback, object_cost,
                created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        batch_data = []
        for error in errors:
            batch_data.append((
                error.run_id,
                error.path_execution_id,
                error.validation_test_id,
                error.severity.value,
                error.error_scope.value,
                error.error_type.value,
                error.object_type,
                error.object_id,
                error.object_guid,
                error.object_fab_no,
                error.object_model_no,
                getattr(error, 'object_phase_no', None),
                error.object_data_code,
                error.object_e2e_group_no,
                error.object_markers,
                error.object_utility_no,
                error.object_item_no,
                error.object_type_no,
                error.object_material_no,
                error.object_flow,
                error.object_is_loopback,
                error.object_cost,
                StringHelper.datetime_to_sqltimestamp(),
                error.notes
            ))
        
        self.db.execute_batch(sql, batch_data)
    
    def _store_path_tags(self, run_id: str, path_id: int, tags: list[dict]):
        """Store path tags for AI training."""
        if not tags:
            return
        
        sql = '''
            INSERT INTO tb_path_tags (
                run_id, path_definition_id, tag_type, tag_code, tag, source, confidence, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        batch_data = []
        for tag in tags:
            batch_data.append((
                run_id,
                path_id,
                tag['tag_type'],
                tag['tag_code'],
                tag['tag'],
                tag['source'],
                tag['confidence'],
                StringHelper.datetime_to_sqltimestamp()
            ))
        
        self.db.execute_batch(sql, batch_data)
    
    def _update_path_execution_validation(self, path: PathValidation, errors: list[ValidationError]):
        """Update path execution with validation results."""
        validation_passed = len(errors) == 0
        error_summary = json.dumps([{
            'test_code': self._test_cache.get(str(error.validation_test_id), {}).get('code', 'UNKNOWN'),
            'severity': error.severity.value,
            'error_type': error.error_type.value,
            'message': error.error_message
        } for error in errors[:10]])  # Limit to first 10 errors
        
        sql = '''
            UPDATE tb_path_executions 
            SET validation_passed = ?, validation_errors = ?
            WHERE id = ?
        '''
        
        self.db.execute(sql, (validation_passed, error_summary, path.execution_id))
    
    def _create_review_flags(self, run_id: str, path: PathValidation, errors: list[ValidationError]):
        """Create review flags for critical issues."""
        critical_errors = [e for e in errors if e.severity == Severity.CRITICAL]
        
        if not critical_errors:
            return
        
        sql = '''
            INSERT INTO tb_run_reviews (
                run_id, flag_type, severity, reason, object_type, object_id, object_guid,
                object_fab_no, object_model_no, object_phase_no, object_e2e_group_no,
                object_data_code, object_markers, object_utility_no, object_item_no,
                object_type_no, object_material_no, object_flow, object_is_loopback,
                object_cost, path_context, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        batch_data = []
        for error in critical_errors:
            path_context = json.dumps({
                'path_id': path.path_id,
                'error_type': error.error_type.value,
                'error_message': error.error_message
            })
            
            batch_data.append((
                run_id,
                'CRITICAL_ERROR',
                error.severity.value,
                f'Critical validation error: {error.error_message}',
                error.object_type,
                error.object_id,
                error.object_guid,
                error.object_fab_no,
                error.object_model_no,
                getattr(error, 'object_phase_no', None),
                error.object_e2e_group_no,
                error.object_data_code,
                error.object_markers,
                error.object_utility_no,
                error.object_item_no,
                error.object_type_no,
                error.object_material_no,
                error.object_flow,
                error.object_is_loopback,
                error.object_cost,
                path_context,
                'OPEN',
                StringHelper.datetime_to_sqltimestamp()
            ))
        
        self.db.execute_batch(sql, batch_data)
    
    def _create_validation_summary(self, results: dict) -> dict:
        """Create a summary of validation results."""
        return {
            'validation_success_rate': (results['paths_passed'] / results['total_paths_validated'] * 100) if results['total_paths_validated'] > 0 else 0,
            'avg_errors_per_path': results['total_errors'] / results['total_paths_validated'] if results['total_paths_validated'] > 0 else 0,
            'most_common_error': results['errors_by_type'].most_common(1)[0] if results['errors_by_type'] else None,
            'severity_breakdown': dict(results['errors_by_severity']),
            'critical_path_count': len([p for p in results['paths_with_errors'] if any(
                s == 'CRITICAL' for s in results['errors_by_severity']
            )]),
            'requires_manual_review': results['critical_errors'] > 0
        }