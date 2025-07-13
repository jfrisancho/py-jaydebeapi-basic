# managers/validation.py

from typing import Optional, Any
from dataclasses import dataclass
from enum import Enum

from db import Database
from string_helper import StringHelper
from sample_models import PathResult


class ValidationSeverity(Enum):
    """Validation error severity levels."""
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class ValidationScope(Enum):
    """Validation scope categories."""
    CONNECTIVITY = 'CONNECTIVITY'
    FLOW = 'FLOW'
    MATERIAL = 'MATERIAL'
    QA = 'QA'
    SCENARIO = 'SCENARIO'


class ValidationTestType(Enum):
    """Types of validation tests."""
    STRUCTURAL = 'STRUCTURAL'
    LOGICAL = 'LOGICAL'
    PERFORMANCE = 'PERFORMANCE'
    COMPLIANCE = 'COMPLIANCE'


@dataclass
class ValidationTest:
    """Defines a validation test."""
    code: str
    name: str
    scope: ValidationScope
    severity: ValidationSeverity
    test_type: ValidationTestType
    is_active: bool
    description: Optional[str] = None


@dataclass
class ValidationError:
    """Represents a validation error."""
    run_id: str
    path_definition_id: Optional[int]
    validation_test_id: Optional[int]
    severity: ValidationSeverity
    error_scope: str
    error_type: str
    object_type: str
    object_id: int
    object_guid: str
    error_message: str
    error_data: Optional[str] = None
    notes: Optional[str] = None
    
    # Object-specific fields
    object_fab_no: Optional[int] = None
    object_model_no: Optional[int] = None
    object_data_code: Optional[int] = None
    object_e2e_group_no: Optional[int] = None
    object_markers: Optional[str] = None
    object_utility_no: Optional[int] = None
    object_item_no: Optional[int] = None
    object_type_no: Optional[int] = None
    object_material_no: Optional[int] = None
    object_flow: Optional[str] = None
    object_is_loopback: Optional[bool] = None
    object_cost: Optional[float] = None


@dataclass
class ValidationResult:
    """Result of path validation."""
    passed: bool
    errors: list[ValidationError]
    warnings: list[ValidationError]
    critical_errors: int
    total_errors: int


class ValidationManager:
    """Comprehensive path validation with configurable tests."""
    
    def __init__(self, db: Database):
        self.db = db
        self._validation_tests = {}
        self._load_validation_tests()
    
    def validate_path(self, run_id: str, path_definition_id: int, 
                     path_result: PathResult) -> ValidationResult:
        """Perform comprehensive validation on a path."""
        errors = []
        warnings = []
        
        # 1. Connectivity validation
        connectivity_errors = self._validate_connectivity(
            run_id, path_definition_id, path_result
        )
        errors.extend(connectivity_errors)
        
        # 2. Utility consistency validation
        utility_errors = self._validate_utility_consistency(
            run_id, path_definition_id, path_result
        )
        errors.extend(utility_errors)
        
        # 3. PoC configuration validation
        poc_errors = self._validate_poc_configuration(
            run_id, path_definition_id, path_result
        )
        errors.extend(poc_errors)
        
        # 4. Structural integrity validation
        structural_errors = self._validate_structural_integrity(
            run_id, path_definition_id, path_result
        )
        errors.extend(structural_errors)
        
        # Separate errors by severity
        critical_errors = [e for e in errors if e.severity == ValidationSeverity.CRITICAL]
        non_critical_errors = [e for e in errors if e.severity != ValidationSeverity.CRITICAL]
        warnings = [e for e in non_critical_errors if e.severity == ValidationSeverity.LOW]
        
        # Store validation errors
        for error in errors:
            self._store_validation_error(error)
        
        return ValidationResult(
            passed=len(critical_errors) == 0,
            errors=non_critical_errors,
            warnings=warnings,
            critical_errors=len(critical_errors),
            total_errors=len(errors)
        )
    
    def _validate_connectivity(self, run_id: str, path_definition_id: int,
                             path_result: PathResult) -> list[ValidationError]:
        """Validate basic connectivity requirements."""
        errors = []
        
        # Check if path has nodes and links
        if not path_result.nodes:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_001',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'NO_NODES',
                'PATH', 0, 'path', 'Path has no nodes'
            ))
        
        if not path_result.links:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_002',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'NO_LINKS',
                'PATH', 0, 'path', 'Path has no links'
            ))
        
        # Check if start and end nodes exist
        if path_result.start_node_id not in path_result.nodes:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_003',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'INVALID_START_NODE',
                'NODE', path_result.start_node_id, f'node_{path_result.start_node_id}',
                'Start node not found in path nodes'
            ))
        
        if path_result.end_node_id not in path_result.nodes:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_004',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'INVALID_END_NODE',
                'NODE', path_result.end_node_id, f'node_{path_result.end_node_id}',
                'End node not found in path nodes'
            ))
        
        # Validate node existence in database
        node_validation_errors = self._validate_nodes_exist(
            run_id, path_definition_id, path_result.nodes
        )
        errors.extend(node_validation_errors)
        
        # Validate link existence and connectivity
        link_validation_errors = self._validate_links_connectivity(
            run_id, path_definition_id, path_result.nodes, path_result.links
        )
        errors.extend(link_validation_errors)
        
        return errors
    
    def _validate_utility_consistency(self, run_id: str, path_definition_id: int,
                                    path_result: PathResult) -> list[ValidationError]:
        """Validate utility consistency along the path."""
        errors = []
        
        if not path_result.utility_nos:
            return errors
        
        # Get utility information for path nodes
        node_utilities = self._fetch_node_utilities(path_result.nodes)
        
        # Check for utility consistency
        prev_utility = None
        for i, node_id in enumerate(path_result.nodes):
            current_utility = node_utilities.get(node_id)
            
            if current_utility is None:
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'UTIL_001',
                    ValidationSeverity.MEDIUM, 'FLOW', 'MISSING_UTILITY',
                    'NODE', node_id, f'node_{node_id}',
                    'Node has no utility information'
                ))
                continue
            
            if prev_utility is not None and current_utility != prev_utility:
                # Check if utility change is valid (requires equipment or special node)
                if not self._is_valid_utility_transition(prev_utility, current_utility):
                    errors.append(self._create_validation_error(
                        run_id, path_definition_id, 'UTIL_002',
                        ValidationSeverity.HIGH, 'FLOW', 'INVALID_UTILITY_TRANSITION',
                        'NODE', node_id, f'node_{node_id}',
                        f'Invalid utility transition from {prev_utility} to {current_utility}',
                        object_utility_no=current_utility
                    ))
            
            prev_utility = current_utility
        
        return errors
    
    def _validate_poc_configuration(self, run_id: str, path_definition_id: int,
                                  path_result: PathResult) -> list[ValidationError]:
        """Validate PoC configuration requirements."""
        errors = []
        
        # Validate start PoC
        start_poc = self._fetch_poc_info(path_result.start_poc_id)
        if start_poc:
            poc_errors = self._validate_single_poc(
                run_id, path_definition_id, start_poc, 'START_POC'
            )
            errors.extend(poc_errors)
        else:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_001',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'MISSING_START_POC',
                'POC', path_result.start_poc_id, f'poc_{path_result.start_poc_id}',
                'Start PoC not found in database'
            ))
        
        # Validate end PoC
        end_poc = self._fetch_poc_info(path_result.end_poc_id)
        if end_poc:
            poc_errors = self._validate_single_poc(
                run_id, path_definition_id, end_poc, 'END_POC'
            )
            errors.extend(poc_errors)
        else:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_002',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'MISSING_END_POC',
                'POC', path_result.end_poc_id, f'poc_{path_result.end_poc_id}',
                'End PoC not found in database'
            ))
        
        return errors
    
    def _validate_structural_integrity(self, run_id: str, path_definition_id: int,
                                     path_result: PathResult) -> list[ValidationError]:
        """Validate structural integrity of the path."""
        errors = []
        
        # Check path length consistency
        if path_result.total_length_mm <= 0:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'STRUCT_001',
                ValidationSeverity.MEDIUM, 'STRUCTURAL', 'INVALID_LENGTH',
                'PATH', 0, 'path',
                f'Invalid path length: {path_result.total_length_mm}mm'
            ))
        
        # Check cost consistency
        if path_result.total_cost < 0:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'STRUCT_002',
                ValidationSeverity.LOW, 'PERFORMANCE', 'NEGATIVE_COST',
                'PATH', 0, 'path',
                f'Negative path cost: {path_result.total_cost}'
            ))
        
        # Check node-link ratio (heuristic check)
        if len(path_result.nodes) > 0 and len(path_result.links) > 0:
            ratio = len(path_result.links) / len(path_result.nodes)
            if ratio > 2.0:  # Arbitrary threshold
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'STRUCT_003',
                    ValidationSeverity.LOW, 'STRUCTURAL', 'UNUSUAL_TOPOLOGY',
                    'PATH', 0, 'path',
                    f'Unusual link-to-node ratio: {ratio:.2f}'
                ))
        
        return errors
    
    def _validate_single_poc(self, run_id: str, path_definition_id: int,
                           poc_info: dict, poc_type: str) -> list[ValidationError]:
        """Validate a single PoC configuration."""
        errors = []
        
        poc_id = poc_info['id']
        
        # Check if PoC is marked as used
        if not poc_info.get('is_used'):
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_003',
                ValidationSeverity.HIGH, 'CONNECTIVITY', 'POC_NOT_USED',
                'POC', poc_id, f'poc_{poc_id}',
                f'{poc_type} PoC is not marked as used',
                object_is_loopback=poc_info.get('is_loopback', False)
            ))
        
        # Check required fields
        if not poc_info.get('markers'):
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_004',
                ValidationSeverity.MEDIUM, 'QA', 'MISSING_MARKERS',
                'POC', poc_id, f'poc_{poc_id}',
                f'{poc_type} PoC missing markers',
                object_markers=poc_info.get('markers')
            ))
        
        if not poc_info.get('reference'):
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_005',
                ValidationSeverity.MEDIUM, 'QA', 'MISSING_REFERENCE',
                'POC', poc_id, f'poc_{poc_id}',
                f'{poc_type} PoC missing reference'
            ))
        
        if poc_info.get('utility_no') is None:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'POC_006',
                ValidationSeverity.LOW, 'FLOW', 'MISSING_UTILITY',
                'POC', poc_id, f'poc_{poc_id}',
                f'{poc_type} PoC missing utility number'
            ))
        
        return errors
    
    def _validate_nodes_exist(self, run_id: str, path_definition_id: int,
                            node_ids: list[int]) -> list[ValidationError]:
        """Validate that all nodes exist in the database."""
        errors = []
        
        if not node_ids:
            return errors
        
        # Check node existence in batches
        placeholders = ', '.join(['?' for _ in node_ids])
        sql = f'SELECT id FROM nw_nodes WHERE id IN ({placeholders})'
        existing_rows = self.db.query(sql, node_ids)
        existing_node_ids = {row[0] for row in existing_rows}
        
        # Find missing nodes
        missing_nodes = set(node_ids) - existing_node_ids
        for node_id in missing_nodes:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_005',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'MISSING_NODE',
                'NODE', node_id, f'node_{node_id}',
                'Node does not exist in database'
            ))
        
        return errors
    
    def _validate_links_connectivity(self, run_id: str, path_definition_id: int,
                                   node_ids: list[int], link_ids: list[int]) -> list[ValidationError]:
        """Validate that links exist and connect the path nodes properly."""
        errors = []
        
        if not link_ids:
            return errors
        
        # Fetch link information
        placeholders = ', '.join(['?' for _ in link_ids])
        sql = f'''
            SELECT id, s_node_id, e_node_id, bidirected 
            FROM nw_links 
            WHERE id IN ({placeholders})
        '''
        link_rows = self.db.query(sql, link_ids)
        existing_links = {row[0]: {'s_node_id': row[1], 'e_node_id': row[2], 'bidirected': row[3]} 
                         for row in link_rows}
        
        # Check for missing links
        missing_links = set(link_ids) - set(existing_links.keys())
        for link_id in missing_links:
            errors.append(self._create_validation_error(
                run_id, path_definition_id, 'CONN_006',
                ValidationSeverity.CRITICAL, 'CONNECTIVITY', 'MISSING_LINK',
                'LINK', link_id, f'link_{link_id}',
                'Link does not exist in database'
            ))
        
        # Validate link connectivity
        node_set = set(node_ids)
        for link_id, link_info in existing_links.items():
            s_node_id = link_info['s_node_id']
            e_node_id = link_info['e_node_id']
            
            if s_node_id not in node_set or e_node_id not in node_set:
                errors.append(self._create_validation_error(
                    run_id, path_definition_id, 'CONN_007',
                    ValidationSeverity.HIGH, 'CONNECTIVITY', 'DISCONNECTED_LINK',
                    'LINK', link_id, f'link_{link_id}',
                    f'Link connects nodes not in path: {s_node_id} -> {e_node_id}'
                ))
        
        return errors
    
    def _fetch_node_utilities(self, node_ids: list[int]) -> dict[int, int]:
        """Fetch utility information for nodes."""
        if not node_ids:
            return {}
        
        placeholders = ', '.join(['?' for _ in node_ids])
        sql = f'SELECT id, utility_no FROM nw_nodes WHERE id IN ({placeholders})'
        rows = self.db.query(sql, node_ids)
        
        return {row[0]: row[1] for row in rows if row[1] is not None}
    
    def _fetch_poc_info(self, poc_id: int) -> Optional[dict]:
        """Fetch PoC information."""
        sql = '''
            SELECT id, equipment_id, node_id, markers, reference, 
                   utility_no, flow, is_used, is_loopback
            FROM tb_equipment_pocs 
            WHERE id = ?
        '''
        rows = self.db.query(sql, [poc_id])
        
        if not rows:
            return None
        
        row = rows[0]
        return {
            'id': row[0],
            'equipment_id': row[1],
            'node_id': row[2],
            'markers': row[3],
            'reference': row[4],
            'utility_no': row[5],
            'flow': row[6],
            'is_used': bool(row[7]),
            'is_loopback': bool(row[8])
        }
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if a utility transition is valid."""
        # Define valid utility transitions (this would be configurable in a real system)
        valid_transitions = {
            # Water utilities
            (1, 2): True,  # Water to Steam
            (2, 1): True,  # Steam to Water (condensation)
            # Gas utilities
            (10, 11): True,  # N2 to Compressed N2
            # Add more transitions as needed
        }
        
        return valid_transitions.get((from_utility, to_utility), False)
    
    def _create_validation_error(self, run_id: str, path_definition_id: int,
                               error_code: str, severity: ValidationSeverity,
                               error_scope: str, error_type: str,
                               object_type: str, object_id: int, object_guid: str,
                               error_message: str, error_data: Optional[str] = None,
                               **object_fields) -> ValidationError:
        """Create a validation error with proper object information."""
        return ValidationError(
            run_id=run_id,
            path_definition_id=path_definition_id,
            validation_test_id=None,  # Could be mapped from error_code
            severity=severity,
            error_scope=error_scope,
            error_type=error_type,
            object_type=object_type,
            object_id=object_id,
            object_guid=object_guid,
            error_message=error_message,
            error_data=error_data,
            **object_fields
        )
    
    def _store_validation_error(self, error: ValidationError) -> None:
        """Store validation error in database."""
        error_data = {
            'run_id': error.run_id,
            'path_definition_id': error.path_definition_id,
            'validation_test_id': error.validation_test_id,
            'severity': error.severity.value,
            'error_scope': error.error_scope,
            'error_type': error.error_type,
            'object_type': error.object_type,
            'object_id': error.object_id,
            'object_guid': error.object_guid,
            'object_fab_no': error.object_fab_no,
            'object_model_no': error.object_model_no,
            'object_data_code': error.object_data_code,
            'object_e2e_group_no': error.object_e2e_group_no,
            'object_markers': error.object_markers,
            'object_utility_no': error.object_utility_no,
            'object_item_no': error.object_item_no,
            'object_type_no': error.object_type_no,
            'object_material_no': error.object_material_no,
            'object_flow': error.object_flow,
            'object_is_loopback': 1 if error.object_is_loopback else 0 if error.object_is_loopback is not None else None,
            'object_cost': error.object_cost,
            'error_message': error.error_message,
            'error_data': error.error_data,
            'notes': error.notes
        }
        
        # Remove None values
        error_data = {k: v for k, v in error_data.items() if v is not None}
        
        columns = list(error_data.keys())
        placeholders = ', '.join(['?' for _ in columns])
        sql = f'INSERT INTO tb_validation_errors ({", ".join(columns)}) VALUES ({placeholders})'
        
        self.db.update(sql, list(error_data.values()))
    
    def _load_validation_tests(self) -> None:
        """Load validation test definitions from database."""
        sql = '''
            SELECT code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests 
            WHERE is_active = 1
        '''
        rows = self.db.query(sql)
        
        for row in rows:
            test = ValidationTest(
                code=row[0],
                name=row[1],
                scope=ValidationScope(row[2]),
                severity=ValidationSeverity(row[3]),
                test_type=ValidationTestType(row[4]),
                is_active=bool(row[5]),
                description=row[6]
            )
            self._validation_tests[test.code] = test
    
    def fetch_validation_errors_by_run(self, run_id: str) -> list[ValidationError]:
        """Fetch all validation errors for a run."""
        sql = '''
            SELECT run_id, path_definition_id, validation_test_id, severity,
                   error_scope, error_type, object_type, object_id, object_guid,
                   object_fab_no, object_model_no, object_data_code, object_e2e_group_no,
                   object_markers, object_utility_no, object_item_no, object_type_no,
                   object_material_no, object_flow, object_is_loopback, object_cost,
                   error_message, error_data, notes
            FROM tb_validation_errors 
            WHERE run_id = ?
            ORDER BY created_at
        '''
        rows = self.db.query(sql, [run_id])
        
        return [ValidationError(
            run_id=row[0],
            path_definition_id=row[1],
            validation_test_id=row[2],
            severity=ValidationSeverity(row[3]),
            error_scope=row[4],
            error_type=row[5],
            object_type=row[6],
            object_id=row[7],
            object_guid=row[8],
            object_fab_no=row[9],
            object_model_no=row[10],
            object_data_code=row[11],
            object_e2e_group_no=row[12],
            object_markers=row[13],
            object_utility_no=row[14],
            object_item_no=row[15],
            object_type_no=row[16],
            object_material_no=row[17],
            object_flow=row[18],
            object_is_loopback=bool(row[19]) if row[19] is not None else None,
            object_cost=row[20],
            error_message=row[21],
            error_data=row[22],
            notes=row[23]
        ) for row in rows]
    
    def get_validation_summary(self, run_id: str) -> dict[str, Any]:
        """Get validation summary statistics for a run."""
        sql = '''
            SELECT 
                COUNT(*) as total_errors,
                SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_errors,
                SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_errors,
                SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_errors,
                SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_errors,
                COUNT(DISTINCT error_scope) as error_scopes,
                COUNT(DISTINCT error_type) as error_types
            FROM tb_validation_errors 
            WHERE run_id = ?
        '''
        rows = self.db.query(sql, [run_id])
        
        if not rows or not rows[0][0]:
            return {
                'total_errors': 0,
                'critical_errors': 0,
                'high_errors': 0,
                'medium_errors': 0,
                'low_errors': 0,
                'error_scopes': 0,
                'error_types': 0
            }
        
        row = rows[0]
        return {
            'total_errors': row[0],
            'critical_errors': row[1],
            'high_errors': row[2],
            'medium_errors': row[3],
            'low_errors': row[4],
            'error_scopes': row[5],
            'error_types': row[6]
        }