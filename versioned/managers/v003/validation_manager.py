# managers/validation.py

from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from enum import Enum

from db import Database
from string_helper import StringHelper


class ValidationSeverity(Enum):
    LOW = 'LOW'
    MEDIUM = 'MEDIUM'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'


class ValidationManager:
    """Comprehensive path validation manager for connectivity and utility consistency."""
    
    def __init__(self, db: Database):
        self.db = db
        self._validation_tests = self._load_validation_tests()
        self._utility_compatibility = self._load_utility_compatibility()
    
    def validate_connectivity(self, run_id: str, path_def: Dict) -> List['ValidationError']:
        """Validate path connectivity and completeness."""
        
        errors = []
        
        # Extract path information
        path_context = self._parse_path_context(path_def.get('path_context', ''))
        nodes = path_context.get('nodes', [])
        links = path_context.get('links', [])
        
        if not nodes or not links:
            errors.append(self._create_validation_error(
                run_id, path_def['id'], 'CONNECTIVITY', 'MISSING_PATH_DATA',
                ValidationSeverity.CRITICAL, 'PATH', 0,
                'Path definition missing nodes or links data'
            ))
            return errors
        
        # Validate node connectivity
        connectivity_errors = self._validate_node_connectivity(run_id, path_def, nodes, links)
        errors.extend(connectivity_errors)
        
        # Validate POC completeness
        poc_errors = self._validate_poc_completeness(run_id, path_def, nodes)
        errors.extend(poc_errors)
        
        # Validate path continuity
        continuity_errors = self._validate_path_continuity(run_id, path_def, nodes, links)
        errors.extend(continuity_errors)
        
        return errors
    
    def validate_utility_consistency(self, run_id: str, path_def: Dict) -> List['ValidationError']:
        """Validate utility consistency along the path."""
        
        errors = []
        
        # Extract path utilities
        utilities_data = self._extract_path_utilities(path_def)
        
        if not utilities_data:
            return errors
        
        # Validate utility transitions
        transition_errors = self._validate_utility_transitions(run_id, path_def, utilities_data)
        errors.extend(transition_errors)
        
        # Validate utility compatibility with equipment
        compatibility_errors = self._validate_utility_equipment_compatibility(
            run_id, path_def, utilities_data
        )
        errors.extend(compatibility_errors)
        
        # Validate utility flow consistency
        flow_errors = self._validate_utility_flow_consistency(run_id, path_def, utilities_data)
        errors.extend(flow_errors)
        
        return errors
    
    def generate_review_flags(self, run_id: str, path_def: Dict, 
                            validation_errors: List['ValidationError']) -> List['ReviewFlag']:
        """Generate review flags based on validation errors."""
        
        flags = []
        
        # Group errors by severity
        critical_errors = [e for e in validation_errors if e.severity == ValidationSeverity.CRITICAL]
        high_errors = [e for e in validation_errors if e.severity == ValidationSeverity.HIGH]
        
        # Create flags for critical errors
        for error in critical_errors:
            flag = self._create_review_flag(
                run_id, 'CRITICAL_ERROR', ValidationSeverity.CRITICAL,
                f'Critical validation error: {error.error_type}',
                error.object_type, error.object_id, error.object_guid,
                error.error_message
            )
            flags.append(flag)
        
        # Create aggregate flag for multiple high-severity errors
        if len(high_errors) > 2:
            flag = self._create_review_flag(
                run_id, 'MULTIPLE_HIGH_ERRORS', ValidationSeverity.HIGH,
                f'Multiple high-severity errors detected ({len(high_errors)} errors)',
                'PATH', path_def['id'], path_def['path_hash'],
                f'Path has {len(high_errors)} high-severity validation errors'
            )
            flags.append(flag)
        
        # Store flags in database
        for flag in flags:
            self._store_review_flag(flag)
        
        return flags
    
    def create_review_flag(self, run_id: str, flag_type: str, severity: str,
                          reason: str, object_type: str, object_id: int,
                          notes: str = None) -> 'ReviewFlag':
        """Create and store a review flag."""
        
        flag = ReviewFlag(
            run_id=run_id,
            flag_type=flag_type,
            severity=ValidationSeverity(severity),
            reason=reason,
            object_type=object_type,
            object_id=object_id,
            object_guid='',  # Would need to fetch from object
            notes=notes,
            created_at=datetime.now()
        )
        
        self._store_review_flag(flag)
        return flag
    
    def _validate_node_connectivity(self, run_id: str, path_def: Dict, 
                                  nodes: List[int], links: List[int]) -> List['ValidationError']:
        """Validate that nodes in the path are properly connected."""
        
        errors = []
        
        # Fetch node details
        node_details = self._fetch_node_details(nodes)
        node_dict = {node['id']: node for node in node_details}
        
        # Fetch link details
        link_details = self._fetch_link_details(links)
        
        # Check that all nodes exist
        for node_id in nodes:
            if node_id not in node_dict:
                errors.append(self._create_validation_error(
                    run_id, path_def['id'], 'CONNECTIVITY', 'MISSING_NODE',
                    ValidationSeverity.CRITICAL, 'NODE', node_id,
                    f'Node {node_id} not found in database'
                ))
        
        # Check that all links exist and connect the nodes properly
        for link in link_details:
            start_node = link['start_node_id']
            end_node = link['end_node_id']
            
            if start_node not in nodes or end_node not in nodes:
                errors.append(self._create_validation_error(
                    run_id, path_def['id'], 'CONNECTIVITY', 'DISCONNECTED_LINK',
                    ValidationSeverity.HIGH, 'LINK', link['id'],
                    f'Link {link["id"]} connects nodes not in path'
                ))
        
        return errors
    
    def _validate_poc_completeness(self, run_id: str, path_def: Dict, 
                                 nodes: List[int]) -> List['ValidationError']:
        """Validate that POCs have required attributes."""
        
        errors = []
        
        # Get POCs associated with the path nodes
        if not nodes:
            return errors
        
        node_placeholders = ','.join(['?' for _ in nodes])
        sql = f'''
            SELECT p.id, p.node_id, p.is_used, p.markers, p.utility_no, 
                   p.reference, p.flow, e.id as equipment_id
            FROM tb_equipment_pocs p
            INNER JOIN tb_equipments e ON p.equipment_id = e.id
            WHERE p.node_id IN ({node_placeholders}) AND p.is_active = 1
        '''
        
        poc_rows = self.db.query(sql, nodes)
        
        for row in poc_rows:
            poc_id, node_id, is_used, markers, utility_no, reference, flow, equipment_id = row
            
            # Check if used POCs have required attributes
            if is_used:
                if not utility_no:
                    errors.append(self._create_validation_error(
                        run_id, path_def['id'], 'CONNECTIVITY', 'MISSING_UTILITY',
                        ValidationSeverity.HIGH, 'POC', poc_id,
                        f'Used POC {poc_id} missing utility assignment'
                    ))
                
                if not markers:
                    errors.append(self._create_validation_error(
                        run_id, path_def['id'], 'CONNECTIVITY', 'MISSING_MARKERS',
                        ValidationSeverity.MEDIUM, 'POC', poc_id,
                        f'Used POC {poc_id} missing markers'
                    ))
                
                if not reference:
                    errors.append(self._create_validation_error(
                        run_id, path_def['id'], 'CONNECTIVITY', 'MISSING_REFERENCE',
                        ValidationSeverity.MEDIUM, 'POC', poc_id,
                        f'Used POC {poc_id} missing reference'
                    ))
        
        return errors
    
    def _validate_path_continuity(self, run_id: str, path_def: Dict,
                                nodes: List[int], links: List[int]) -> List['ValidationError']:
        """Validate that the path forms a continuous route."""
        
        errors = []
        
        if len(nodes) < 2 or len(links) < 1:
            return errors
        
        # Fetch link connectivity
        link_details = self._fetch_link_details(links)
        
        # Build adjacency map
        adjacency = {}
        for link in link_details:
            start_id = link['start_node_id']
            end_id = link['end_node_id']
            
            if start_id not in adjacency:
                adjacency[start_id] = []
            if end_id not in adjacency:
                adjacency[end_id] = []
            
            adjacency[start_id].append(end_id)
            if link['bidirected'] == 'Y':
                adjacency[end_id].append(start_id)
        
        # Check path continuity
        for i in range(len(nodes) - 1):
            current_node = nodes[i]
            next_node = nodes[i + 1]
            
            if (current_node not in adjacency or 
                next_node not in adjacency[current_node]):
                errors.append(self._create_validation_error(
                    run_id, path_def['id'], 'CONNECTIVITY', 'PATH_DISCONTINUITY',
                    ValidationSeverity.CRITICAL, 'PATH', 0,
                    f'No connection between nodes {current_node} and {next_node}'
                ))
        
        return errors
    
    def _validate_utility_transitions(self, run_id: str, path_def: Dict,
                                    utilities_data: Dict) -> List['ValidationError']:
        """Validate utility transitions along the path."""
        
        errors = []
        utilities = utilities_data.get('utilities', [])
        
        if len(utilities) < 2:
            return errors
        
        # Check for valid utility transitions
        for i in range(len(utilities) - 1):
            current_utility = utilities[i]
            next_utility = utilities[i + 1]
            
            if current_utility != next_utility:
                # Check if transition is valid
                if not self._is_valid_utility_transition(current_utility, next_utility):
                    errors.append(self._create_validation_error(
                        run_id, path_def['id'], 'UTILITY', 'INVALID_TRANSITION',
                        ValidationSeverity.HIGH, 'PATH', 0,
                        f'Invalid utility transition from {current_utility} to {next_utility}'
                    ))
        
        return errors
    
    def _validate_utility_equipment_compatibility(self, run_id: str, path_def: Dict,
                                                utilities_data: Dict) -> List['ValidationError']:
        """Validate utility compatibility with equipment types."""
        
        errors = []
        
        # This would require fetching equipment details and checking compatibility
        # Implementation depends on specific business rules
        
        return errors
    
    def _validate_utility_flow_consistency(self, run_id: str, path_def: Dict,
                                         utilities_data: Dict) -> List['ValidationError']:
        """Validate utility flow consistency (IN/OUT directions)."""
        
        errors = []
        
        # This would check flow directions and consistency
        # Implementation depends on specific flow rules
        
        return errors
    
    def _load_validation_tests(self) -> Dict[str, Dict]:
        """Load validation test configurations."""
        
        sql = '''
            SELECT code, name, scope, severity, test_type, is_active, description
            FROM tb_validation_tests
            WHERE is_active = 1
        '''
        
        rows = self.db.query(sql)
        
        tests = {}
        for row in rows:
            tests[row[0]] = {
                'code': row[0],
                'name': row[1],
                'scope': row[2],
                'severity': row[3],
                'test_type': row[4],
                'is_active': bool(row[5]),
                'description': row[6]
            }
        
        return tests
    
    def _load_utility_compatibility(self) -> Dict[Tuple[int, int], bool]:
        """Load utility compatibility matrix."""
        
        # This would load utility transition rules from a configuration table
        # For now, return a basic compatibility matrix
        
        compatibility = {
            # (from_utility, to_utility): is_valid
            (1, 1): True,  # Water to Water
            (1, 2): True,  # Water to Steam (heating)
            (2, 1): True,  # Steam to Water (condensation)
            (2, 2): True,  # Steam to Steam
            (3, 3): True,  # Gas to Gas
            (4, 4): True,  # Air to Air
        }
        
        return compatibility
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if utility transition is valid."""
        
        # Same utility is always valid
        if from_utility == to_utility:
            return True
        
        # Check compatibility matrix
        return self._utility_compatibility.get((from_utility, to_utility), False)
    
    def _parse_path_context(self, path_context: str) -> Dict[str, List[int]]:
        """Parse path context string to extract nodes and links."""
        
        nodes = []
        links = []
        
        if path_context:
            parts = path_context.split('|')
            
            for part in parts:
                if part.startswith('nodes:'):
                    node_str = part.replace('nodes:', '')
                    if node_str:
                        nodes = [int(x.strip()) for x in node_str.split(',') if x.strip()]
                elif part.startswith('links:'):
                    link_str = part.replace('links:', '')
                    if link_str:
                        links = [int(x.strip()) for x in link_str.split(',') if x.strip()]
        
        return {'nodes': nodes, 'links': links}
    
    def _extract_path_utilities(self, path_def: Dict) -> Dict[str, List]:
        """Extract utility information from path definition."""
        
        utilities = []
        references = []
        
        if path_def.get('utilities_scope'):
            utility_str = path_def['utilities_scope']
            if utility_str:
                utilities = [int(x.strip()) for x in utility_str.split(',') if x.strip().isdigit()]
        
        if path_def.get('references_scope'):
            ref_str = path_def['references_scope']
            if ref_str:
                references = [x.strip() for x in ref_str.split(',') if x.strip()]
        
        return {
            'utilities': utilities,
            'references': references
        }
    
    def _fetch_node_details(self, node_ids: List[int]) -> List[Dict]:
        """Fetch detailed node information."""
        
        if not node_ids:
            return []
        
        placeholders = ','.join(['?' for _ in node_ids])
        sql = f'''
            SELECT id, guid, fab_no, model_no, data_code, utility_no,
                   e2e_group_no, e2e_header_id, item_no, markers, nwo_type_no
            FROM nw_nodes
            WHERE id IN ({placeholders})
        '''
        
        rows = self.db.query(sql, node_ids)
        
        return [
            {
                'id': row[0],
                'guid': row[1],
                'fab_no': row[2],
                'model_no': row[3],
                'data_code': row[4],
                'utility_no': row[5],
                'e2e_group_no': row[6],
                'e2e_header_id': row[7],
                'item_no': row[8],
                'markers': row[9],
                'nwo_type_no': row[10]
            }
            for row in rows
        ]
    
    def _fetch_link_details(self, link_ids: List[int]) -> List[Dict]:
        """Fetch detailed link information."""
        
        if not link_ids:
            return []
        
        placeholders = ','.join(['?' for _ in link_ids])
        sql = f'''
            SELECT id, start_node_id, end_node_id, guid, bidirected, cost, nwo_type_no
            FROM nw_links
            WHERE id IN ({placeholders})
        '''
        
        rows = self.db.query(sql, link_ids)
        
        return [
            {
                'id': row[0],
                'start_node_id': row[1],
                'end_node_id': row[2],
                'guid': row[3],
                'bidirected': row[4],
                'cost': row[5],
                'nwo_type_no': row[6]
            }
            for row in rows
        ]
    
    def _create_validation_error(self, run_id: str, path_def_id: int, error_scope: str,
                               error_type: str, severity: ValidationSeverity,
                               object_type: str, object_id: int, 
                               error_message: str) -> 'ValidationError':
        """Create a validation error record."""
        
        error = ValidationError(
            run_id=run_id,
            path_definition_id=path_def_id,
            severity=severity,
            error_scope=error_scope,
            error_type=error_type,
            object_type=object_type,
            object_id=object_id,
            object_guid='',  # Would need to fetch
            error_message=error_message,
            created_at=datetime.now()
        )
        
        # Store in database
        self._store_validation_error(error)
        
        return error
    
    def _create_review_flag(self, run_id: str, flag_type: str, severity: ValidationSeverity,
                          reason: str, object_type: str, object_id: int, object_guid: str,
                          notes: str) -> 'ReviewFlag':
        """Create a review flag."""
        
        return ReviewFlag(
            run_id=run_id,
            flag_type=flag_type,
            severity=severity,
            reason=reason,
            object_type=object_type,
            object_id=object_id,
            object_guid=object_guid,
            notes=notes,
            status='OPEN',
            created_at=datetime.now()
        )
    
    def _store_validation_error(self, error: 'ValidationError'):
        """Store validation error in database."""
        
        sql = '''
            INSERT INTO tb_validation_errors (
                run_id, path_definition_id, severity, error_scope, error_type,
                object_type, object_id, object_guid, error_message, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            error.run_id,
            error.path_definition_id,
            error.severity.value,
            error.error_scope,
            error.error_type,
            error.object_type,
            error.object_id,
            error.object_guid,
            error.error_message,
            error.created_at
        ]
        
        self.db.update(sql, params)
    
    def _store_review_flag(self, flag: 'ReviewFlag'):
        """Store review flag in database."""
        
        sql = '''
            INSERT INTO tb_review_flags (
                run_id, flag_type, severity, reason, object_type, object_id,
                object_guid, status, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        params = [
            flag.run_id,
            flag.flag_type,
            flag.severity.value,
            flag.reason,
            flag.object_type,
            flag.object_id,
            flag.object_guid,
            flag.status,
            flag.created_at,
            flag.notes
        ]
        
        self.db.update(sql, params)


@dataclass
class ValidationError:
    """Validation error record."""
    run_id: str
    path_definition_id: int
    severity: ValidationSeverity
    error_scope: str
    error_type: str
    object_type: str
    object_id: int
    object_guid: str
    error_message: str
    created_at: datetime
    validation_test_id: Optional[int] = None
    error_data: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class ReviewFlag:
    """Review flag record."""
    run_id: str
    flag_type: str
    severity: ValidationSeverity
    reason: str
    object_type: str
    object_id: int
    object_guid: str
    notes: Optional[str]
    status: str = 'OPEN'
    created_at: datetime = None
    assigned_to: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None