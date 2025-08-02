from dataclasses import dataclass
from typing import Optional


class UtilityValidator:
    """Focused utility consistency validator for semiconductor piping"""
    
    def __init__(self, db):
        self.db = db
    
    def _validate_utility_consistency(
        self,
        run_id: str,
        data: 'PathValidation'
    ) -> list['ValidationError']:
        """Validate utility consistency - optimized and focused on critical issues."""
        if data.utilities_scope:
            return []
        
        if not data.network:
            return []
        
        # Single pass to collect validation data
        validation_data = self._analyze_path_utilities(data.network)
        
        errors = []
        
        # Only validate critical issues that are actually actionable
        errors.extend(self._validate_critical_missing_utilities(run_id, data, validation_data))
        errors.extend(self._validate_invalid_direct_connections(run_id, data, validation_data))
        errors.extend(self._validate_orphaned_utilities(run_id, data, validation_data))
        
        return errors
    
    def _analyze_path_utilities(self, network: list['PathLinkRecords']) -> dict:
        """Single pass analysis of path utility patterns"""
        analysis = {
            'used_equipment_poc_without_utility': [],
            'direct_utility_violations': [],
            'utility_sequence': [],
            'equipment_logical_positions': set()
        }
        
        # Build utility sequence for the entire path
        path_nodes = []
        for i, record in enumerate(network):
            if i == 0:
                path_nodes.append(record.s_node)
            path_nodes.append(record.e_node)
        
        # Analyze each node and collect issues
        for i, node in enumerate(path_nodes):
            # Track utility sequence
            analysis['utility_sequence'].append({
                'position': i,
                'node_id': node.id,
                'utility': node.utility_no,
                'is_equipment_logical': node.is_equipment_logical,
                'is_equipment_poc': node.is_equipment_poc,
                'is_used': node.is_used
            })
            
            # Track equipment logical node positions
            if node.is_equipment_logical:
                analysis['equipment_logical_positions'].add(i)
            
            # Critical issue: Used equipment PoC without utility
            if node.is_equipment_poc and node.is_used and not node.utility_no:
                analysis['used_equipment_poc_without_utility'].append(node)
        
        # Check for direct utility violations in links
        for record in network:
            if (record.s_node.utility_no and record.e_node.utility_no and 
                record.s_node.utility_no != record.e_node.utility_no and
                not record.s_node.is_equipment_logical and 
                not record.e_node.is_equipment_logical):
                
                analysis['direct_utility_violations'].append(record)
        
        return analysis
    
    def _validate_critical_missing_utilities(
        self,
        run_id: str,
        data: 'PathValidation',
        analysis: dict
    ) -> list['ValidationError']:
        """Validate critical missing utilities - equipment PoC that are used must have utilities"""
        errors = []
        
        for node in analysis['used_equipment_poc_without_utility']:
            errors.append(self._create_validation_error(
                run_id,
                data.execution_id,
                'PATH_UTY_001',
                'CRITICAL',
                'MISSING_UTILITY',
                'NODE',
                node.id,
                error_message=f'Used equipment PoC node {node.id} missing utility assignment',
                error_data=self._create_error_context({
                    'path_id': data.path_id,
                    'node_id': node.id,
                    'issue': 'Equipment PoC node is connected to piping but has no utility assigned'
                })
            ))
        
        return errors
    
    def _validate_invalid_direct_connections(
        self,
        run_id: str,
        data: 'PathValidation',
        analysis: dict
    ) -> list['ValidationError']:
        """Validate direct connections between different utilities without equipment separation"""
        errors = []
        
        for record in analysis['direct_utility_violations']:
            errors.append(self._create_validation_error(
                run_id,
                data.execution_id,
                'PATH_UTY_002',
                'CRITICAL',
                'DIRECT_UTILITY_CONNECTION',
                'LINK',
                record.link.id,
                error_message=f'Direct connection between utilities {record.s_node.utility_no} and {record.e_node.utility_no}',
                error_data=self._create_error_context({
                    'path_id': data.path_id,
                    'link_id': record.link.id,
                    's_node_id': record.s_node.id,
                    'e_node_id': record.e_node.id,
                    's_utility': record.s_node.utility_no,
                    'e_utility': record.e_node.utility_no,
                    'issue': 'Different utilities connected directly without equipment separation'
                })
            ))
        
        return errors
    
    def _validate_orphaned_utilities(
        self,
        run_id: str,
        data: 'PathValidation',
        analysis: dict
    ) -> list['ValidationError']:
        """Validate orphaned utility assignments - single nodes with different utilities"""
        errors = []
        sequence = analysis['utility_sequence']
        
        if len(sequence) < 3:
            return errors
        
        # Look for isolated utility nodes (different from neighbors, not equipment logical)
        for i in range(1, len(sequence) - 1):
            current = sequence[i]
            prev_node = sequence[i-1]
            next_node = sequence[i+1]
            
            if (current['utility'] and 
                not current['is_equipment_logical'] and
                current['utility'] != prev_node['utility'] and 
                current['utility'] != next_node['utility'] and
                prev_node['utility'] == next_node['utility']):
                
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_003',
                    'WARNING',
                    'ORPHANED_UTILITY',
                    'NODE',
                    current['node_id'],
                    object_utility_no=current['utility'],
                    error_message=f'Node {current["node_id"]} has isolated utility {current["utility"]}',
                    error_data=self._create_error_context({
                        'path_id': data.path_id,
                        'node_id': current['node_id'],
                        'node_utility': current['utility'],
                        'surrounding_utility': prev_node['utility'],
                        'position': current['position'],
                        'issue': 'Single node with different utility may indicate data entry error'
                    })
                ))
        
        return errors
    
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
