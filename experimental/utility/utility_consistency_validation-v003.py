from dataclasses import dataclass
from enum import Enum
from typing import Optional


@dataclass
class UtilitySegment:
    """Represents a utility segment in a path"""
    utility_no: int
    start_index: int
    end_index: int
    nodes: list[int]


class UtilityValidator:
    """Optimized utility consistency validator"""
    
    def __init__(self, db):
        self.db = db
        self._allowed_transitions_cache = None
    
    def _validate_utility_consistency(
        self,
        run_id: str,
        data: 'PathValidation'
    ) -> list['ValidationError']:
        """Validate utility consistency along the path - optimized version."""
        if data.utilities_scope:
            return []
        
        errors = []
        network_length = len(data.network)
        
        if network_length == 0:
            return errors
        
        # Single pass through the network data to collect all validation info
        validation_context = self._build_validation_context(data.network)
        
        # Run all validations using the pre-built context
        errors.extend(self._validate_node_utilities(run_id, data, validation_context))
        errors.extend(self._validate_utility_segments(run_id, data, validation_context))
        errors.extend(self._validate_utility_transitions(run_id, data, validation_context))
        
        return errors
    
    def _build_validation_context(self, network: list['PathLinkRecords']) -> dict:
        """Build context for validation in a single pass through network data"""
        context = {
            'nodes_by_id': {},
            'utility_segments': [],
            'transitions': [],
            'equipment_logical_nodes': set(),
            'equipment_poc_nodes': set(),
            'nodes_without_utility': [],
            'utility_mismatches': []
        }
        
        # Single pass to collect all node information and identify patterns
        for i, record in enumerate(network):
            # Collect unique nodes
            for node in [record.s_node, record.e_node]:
                if node.id not in context['nodes_by_id']:
                    context['nodes_by_id'][node.id] = node
                    
                    # Categorize nodes
                    if node.is_equipment_logical:
                        context['equipment_logical_nodes'].add(node.id)
                    elif node.is_equipment_poc:
                        context['equipment_poc_nodes'].add(node.id)
                    
                    # Check for missing utilities
                    if self._should_have_utility(node) and not node.utility_no:
                        context['nodes_without_utility'].append(node)
            
            # Check for utility mismatches between connected nodes
            if (record.s_node.utility_no and record.e_node.utility_no and 
                record.s_node.utility_no != record.e_node.utility_no):
                context['utility_mismatches'].append(record)
            
            # Collect transitions for segment analysis
            if i > 0:
                prev_record = network[i-1]
                context['transitions'].append({
                    'from_utility': prev_record.e_node.utility_no,
                    'to_utility': record.s_node.utility_no,
                    'from_node': prev_record.e_node,
                    'to_node': record.s_node,
                    'link_index': i
                })
        
        # Build utility segments
        context['utility_segments'] = self._build_utility_segments(network)
        
        return context
    
    def _should_have_utility(self, node: 'NodeData') -> bool:
        """Determine if a node should have a utility assigned"""
        # Equipment logical nodes don't require utilities
        if node.is_equipment_logical:
            return False
            
        # Equipment PoC nodes that are used must have utilities
        if node.is_equipment_poc and node.is_used:
            return True
            
        # Regular nodes (non-logical, non-equipment) should have utilities
        if not node.is_logical and not node.is_equipment:
            return True
            
        return False
    
    def _build_utility_segments(self, network: list['PathLinkRecords']) -> list[UtilitySegment]:
        """Build utility segments from the network path"""
        if not network:
            return []
        
        segments = []
        current_utility = None
        segment_start = 0
        segment_nodes = []
        
        # Process all nodes in the path
        all_nodes = []
        for i, record in enumerate(network):
            if i == 0:
                all_nodes.append(record.s_node)
            all_nodes.append(record.e_node)
        
        for i, node in enumerate(all_nodes):
            node_utility = node.utility_no if not node.is_equipment_logical else None
            
            # Start new segment or continue current one
            if current_utility is None:
                if node_utility is not None:
                    current_utility = node_utility
                    segment_start = i
                    segment_nodes = [node.id]
            elif node_utility != current_utility:
                # End current segment if we have one
                if current_utility is not None and segment_nodes:
                    segments.append(UtilitySegment(
                        utility_no=current_utility,
                        start_index=segment_start,
                        end_index=i-1,
                        nodes=segment_nodes.copy()
                    ))
                
                # Start new segment
                if node_utility is not None:
                    current_utility = node_utility
                    segment_start = i
                    segment_nodes = [node.id]
                else:
                    current_utility = None
                    segment_nodes = []
            else:
                # Continue current segment
                if node_utility is not None:
                    segment_nodes.append(node.id)
        
        # Close final segment
        if current_utility is not None and segment_nodes:
            segments.append(UtilitySegment(
                utility_no=current_utility,
                start_index=segment_start,
                end_index=len(all_nodes)-1,
                nodes=segment_nodes
            ))
        
        return segments
    
    def _validate_node_utilities(
        self,
        run_id: str,
        data: 'PathValidation',
        context: dict
    ) -> list['ValidationError']:
        """Validate that nodes have required utilities"""
        errors = []
        
        # PATH_UTY_001: Missing utilities
        for node in context['nodes_without_utility']:
            errors.append(self._create_validation_error(
                run_id,
                data.execution_id,
                'PATH_UTY_001',
                'CRITICAL',
                'MISSING_UTILITY',
                'NODE',
                node.id,
                object_utility_no=node.utility_no,
                error_message=f'Node {node.id} missing required utility code',
                error_data=self._create_error_context({
                    'path_id': data.path_id,
                    'node_id': node.id,
                    'node_type': node.nwo_type_no,
                    'data_code': node.data_code,
                    'is_equipment_poc': node.is_equipment_poc,
                    'is_used': node.is_used
                })
            ))
        
        # PATH_UTY_002: Direct utility mismatches (not separated by equipment logical)
        for record in context['utility_mismatches']:
            # Check if this is a valid transition through equipment logical node
            if not self._is_valid_utility_connection(record.s_node, record.e_node, context):
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_002',
                    'CRITICAL',
                    'UTILITY_MISMATCH',
                    'LINK',
                    record.link.id,
                    error_message=f'Invalid utility connection between nodes {record.s_node.id} and {record.e_node.id}',
                    error_data=self._create_error_context({
                        'path_id': data.path_id,
                        'link_id': record.link.id,
                        's_node_id': record.s_node.id,
                        'e_node_id': record.e_node.id,
                        's_utility': record.s_node.utility_no,
                        'e_utility': record.e_node.utility_no
                    })
                ))
        
        return errors
    
    def _validate_utility_segments(
        self,
        run_id: str,
        data: 'PathValidation',
        context: dict
    ) -> list['ValidationError']:
        """Validate utility segment integrity"""
        errors = []
        
        # PATH_UTY_006: Check for inconsistent utilities within segments
        for segment in context['utility_segments']:
            segment_nodes = [context['nodes_by_id'][node_id] for node_id in segment.nodes]
            
            for node in segment_nodes:
                if (node.utility_no and 
                    node.utility_no != segment.utility_no and 
                    not node.is_equipment_logical):
                    
                    errors.append(self._create_validation_error(
                        run_id,
                        data.execution_id,
                        'PATH_UTY_006',
                        'WARNING',
                        'SEGMENT_INCONSISTENCY',
                        'NODE',
                        node.id,
                        object_utility_no=node.utility_no,
                        error_message=f'Node {node.id} utility {node.utility_no} inconsistent with segment utility {segment.utility_no}',
                        error_data=self._create_error_context({
                            'path_id': data.path_id,
                            'node_id': node.id,
                            'node_utility': node.utility_no,
                            'segment_utility': segment.utility_no,
                            'segment_start': segment.start_index,
                            'segment_end': segment.end_index
                        })
                    ))
        
        return errors
    
    def _validate_utility_transitions(
        self,
        run_id: str,
        data: 'PathValidation',
        context: dict
    ) -> list['ValidationError']:
        """Validate utility transitions between segments"""
        errors = []
        
        segments = context['utility_segments']
        if len(segments) < 2:
            return errors
        
        # PATH_UTY_003: Check transitions between segments
        for i in range(len(segments) - 1):
            current_segment = segments[i]
            next_segment = segments[i + 1]
            
            if not self._is_valid_utility_transition(current_segment.utility_no, next_segment.utility_no):
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_003',
                    'CRITICAL',
                    'INVALID_TRANSITION',
                    'PATH',
                    data.path_id,
                    error_message=f'Invalid utility transition from {current_segment.utility_no} to {next_segment.utility_no}',
                    error_data=self._create_error_context({
                        'path_id': data.path_id,
                        'from_utility': current_segment.utility_no,
                        'to_utility': next_segment.utility_no,
                        'from_segment_end': current_segment.end_index,
                        'to_segment_start': next_segment.start_index
                    })
                ))
        
        return errors
    
    def _is_valid_utility_connection(self, s_node: 'NodeData', e_node: 'NodeData', context: dict) -> bool:
        """Check if utility connection between two nodes is valid"""
        # Same utility is always valid
        if s_node.utility_no == e_node.utility_no:
            return True
            
        # Connection through equipment logical nodes is valid
        if s_node.is_equipment_logical or e_node.is_equipment_logical:
            return True
            
        # Check if it's a valid utility transition
        return self._is_valid_utility_transition(s_node.utility_no, e_node.utility_no)
    
    def _is_valid_utility_transition(self, from_utility: int, to_utility: int) -> bool:
        """Check if transition between utilities is allowed"""
        if not from_utility or not to_utility:
            return True
            
        if from_utility == to_utility:
            return True
            
        # Get allowed transitions (cached)
        allowed_transitions = self._fetch_allowed_utility_transitions()
        
        if not allowed_transitions:
            # If no transition rules defined, allow all transitions
            return True
            
        return (from_utility, to_utility) in allowed_transitions
    
    def _fetch_allowed_utility_transitions(self) -> set[tuple[int, int]]:
        """Fetch allowed utility transitions from database (cached)"""
        if self._allowed_transitions_cache is not None:
            return self._allowed_transitions_cache
        
        sql = '''
            SELECT from_utility_no, to_utility_no 
            FROM tb_utility_transitions 
            WHERE is_active = 1
        '''
        
        try:
            rows = self.db.query(sql)
            self._allowed_transitions_cache = {(int(row[0]), int(row[1])) for row in rows if row[0] and row[1]}
        except Exception:
            # If transition table doesn't exist or query fails, allow all transitions
            self._allowed_transitions_cache = set()
        
        return self._allowed_transitions_cache
    
    def _create_validation_error(
        self,
        run_id: str,
        execution_id: str,
        error_code: str,
        severity: str,
        error_type: str,
        object_type: str,
        object_id: int,
        object_utility_no: Optional[int] = None,
        error_message: str = '',
        error_data: Optional[str] = None
    ) -> 'ValidationError':
        """Create a validation error object"""
        # This should match your existing ValidationError structure
        return {
            'run_id': run_id,
            'execution_id': execution_id,
            'error_code': error_code,
            'severity': severity,
            'error_type': error_type,
            'object_type': object_type,
            'object_id': object_id,
            'object_utility_no': object_utility_no,
            'error_message': error_message,
            'error_data': error_data
        }
    
    def _create_error_context(self, context_data: dict) -> str:
        """Create JSON string for error context"""
        try:
            import json
            return json.dumps(context_data, default=str)
        except Exception:
            return str(context_data)
