You're right - focus on consistency. Let me define clear, practical rules for well-defined segments:

## Core Validation Rules:

1. **PATH_UTY_001 - Segment Completeness**: Every segment must have a defined utility
   - No null utilities within a segment (except equipment logical nodes as separators)
   - Equipment PoCs define segment utility when present

2. **PATH_UTY_002 - Segment Consistency**: Within each segment, all nodes must have the same utility
   - Exception: Equipment logical nodes (data_code=15000, nwo_type_no=1) can be null as separators
   - Exception: PoC/forced nodes (nwo_type_no=2,3) inherit from their segment

3. **PATH_UTY_003 - Valid Transitions**: Utility changes only allowed at equipment boundaries
   - Direct utility changes between regular nodes are invalid
   - Equipment logical nodes properly separate different utility segments

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class UtilitySegment:
    start_idx: int
    end_idx: int
    expected_utility: Optional[int]
    nodes_with_issues: list[tuple[int, str]]  # (node_id, issue_type)

class OptimizedUtilityValidator:
    
    def _validate_utility_consistency(
        self,
        run_id: str,
        data: PathValidation
    ) -> list[ValidationError]:
        """Validate utility consistency with focus on well-defined segments."""
        if data.utilities_scope:
            return []
        
        errors = []
        segments = self._identify_utility_segments(data.network)
        
        for segment in segments:
            errors.extend(self._validate_segment_consistency(run_id, data, segment))
        
        errors.extend(self._validate_segment_transitions(run_id, data, segments))
        
        return errors
    
    def _identify_utility_segments(self, network: list[PathLinkRecords]) -> list[UtilitySegment]:
        """Identify utility segments separated by equipment logical nodes."""
        if not network:
            return []
        
        segments = []
        segment_start = 0
        
        # Process all nodes in sequence (start node of first record, then end nodes)
        all_nodes = [network[0].s_node] + [record.e_node for record in network]
        
        for idx, node in enumerate(all_nodes):
            is_equipment_logical = (node.data_code == 15000 and node.nwo_type_no == 1)
            
            if is_equipment_logical:
                # End current segment
                if idx > segment_start:
                    segment = self._analyze_segment(all_nodes[segment_start:idx])
                    if segment:
                        segment.start_idx = segment_start
                        segment.end_idx = idx - 1
                        segments.append(segment)
                
                # Start new segment after this equipment logical node
                segment_start = idx + 1
        
        # Handle final segment
        if segment_start < len(all_nodes):
            segment = self._analyze_segment(all_nodes[segment_start:])
            if segment:
                segment.start_idx = segment_start
                segment.end_idx = len(all_nodes) - 1
                segments.append(segment)
        
        return segments
    
    def _analyze_segment(self, nodes: list[NodeData]) -> Optional[UtilitySegment]:
        """Analyze a segment to determine expected utility and identify issues."""
        if not nodes:
            return None
        
        # Find utilities in this segment
        utilities = []
        equipment_poc_utilities = []
        nodes_with_issues = []
        
        for node in nodes:
            is_equipment_poc = (node.data_code == 15000 and node.nwo_type_no == 2)
            is_special_node = node.nwo_type_no in [2, 3]  # PoC or forced connectivity
            is_regular_node = not is_special_node
            
            if node.utility_no:
                utilities.append(node.utility_no)
                if is_equipment_poc:
                    equipment_poc_utilities.append(node.utility_no)
            elif is_regular_node:
                # Regular nodes without utility are problematic
                nodes_with_issues.append((node.id, 'missing_utility'))
        
        # Determine expected utility (equipment PoC takes priority)
        expected_utility = None
        if equipment_poc_utilities:
            # Check equipment PoC consistency
            unique_poc_utilities = list(set(equipment_poc_utilities))
            if len(unique_poc_utilities) == 1:
                expected_utility = unique_poc_utilities[0]
            else:
                # Multiple different utilities in equipment PoCs - this is an issue
                for node in nodes:
                    if (node.data_code == 15000 and node.nwo_type_no == 2 and 
                        node.utility_no in unique_poc_utilities):
                        nodes_with_issues.append((node.id, 'conflicting_equipment_poc'))
        elif utilities:
            # Use most common utility if no equipment PoC
            utility_counts = {}
            for utility in utilities:
                utility_counts[utility] = utility_counts.get(utility, 0) + 1
            expected_utility = max(utility_counts.items(), key=lambda x: x[1])[0]
        
        # Find inconsistent utilities
        if expected_utility:
            for node in nodes:
                if (node.utility_no and 
                    node.utility_no != expected_utility and 
                    not (node.data_code == 15000 and node.nwo_type_no == 2)):  # Skip equipment PoC conflicts (already handled)
                    nodes_with_issues.append((node.id, 'inconsistent_utility'))
        
        return UtilitySegment(
            start_idx=0,  # Will be set by caller
            end_idx=0,    # Will be set by caller
            expected_utility=expected_utility,
            nodes_with_issues=nodes_with_issues
        )
    
    def _validate_segment_consistency(
        self,
        run_id: str,
        data: PathValidation,
        segment: UtilitySegment
    ) -> list[ValidationError]:
        """Validate consistency within a segment."""
        errors = []
        
        # PATH_UTY_001: Segment must have defined utility
        if not segment.expected_utility:
            errors.append(self._create_validation_error(
                run_id,
                data.execution_id,
                'PATH_UTY_001',
                Severity.CRITICAL,
                ErrorType.MISSING_UTILITY,
                ObjectType.PATH,
                data.path_id,
                error_message=f'Segment from {segment.start_idx} to {segment.end_idx} has no defined utility',
                error_data=ContextHelper.to_json({
                    'path_id': data.path_id,
                    'segment_start': segment.start_idx,
                    'segment_end': segment.end_idx,
                }),
            ))
        
        # PATH_UTY_002: Handle specific node issues within segment
        for node_id, issue_type in segment.nodes_with_issues:
            if issue_type == 'missing_utility':
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_002',
                    Severity.CRITICAL,
                    ErrorType.MISSING_UTILITY,
                    ObjectType.NODE,
                    node_id,
                    object_utility_no=segment.expected_utility,
                    error_message=f'Node {node_id} missing utility in segment (expected: {segment.expected_utility})',
                    error_data=ContextHelper.to_json({
                        'path_id': data.path_id,
                        'node_id': node_id,
                        'expected_utility': segment.expected_utility,
                        'segment_start': segment.start_idx,
                        'segment_end': segment.end_idx,
                    }),
                ))
            
            elif issue_type == 'inconsistent_utility':
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_002',
                    Severity.CRITICAL,
                    ErrorType.UTILITY_MISMATCH,
                    ObjectType.NODE,
                    node_id,
                    error_message=f'Node {node_id} has inconsistent utility in segment (expected: {segment.expected_utility})',
                    error_data=ContextHelper.to_json({
                        'path_id': data.path_id,
                        'node_id': node_id,
                        'expected_utility': segment.expected_utility,
                        'segment_start': segment.start_idx,
                        'segment_end': segment.end_idx,
                    }),
                ))
            
            elif issue_type == 'conflicting_equipment_poc':
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_002',
                    Severity.CRITICAL,
                    ErrorType.UTILITY_MISMATCH,
                    ObjectType.NODE,
                    node_id,
                    error_message=f'Equipment PoC {node_id} has conflicting utility with other PoCs in segment',
                    error_data=ContextHelper.to_json({
                        'path_id': data.path_id,
                        'node_id': node_id,
                        'segment_start': segment.start_idx,
                        'segment_end': segment.end_idx,
                    }),
                ))
        
        return errors
    
    def _validate_segment_transitions(
        self,
        run_id: str,
        data: PathValidation,
        segments: list[UtilitySegment]
    ) -> list[ValidationError]:
        """Validate that utility changes only occur at proper segment boundaries."""
        errors = []
        
        # PATH_UTY_003: Validate transitions between segments
        for i in range(len(segments) - 1):
            current_segment = segments[i]
            next_segment = segments[i + 1]
            
            if (current_segment.expected_utility and 
                next_segment.expected_utility and 
                current_segment.expected_utility != next_segment.expected_utility):
                
                # This is a valid transition (utilities can change between segments)
                # but we might want to log it for audit purposes
                pass
        
        # Check for invalid direct transitions within what should be a single segment
        # This catches cases where nodes change utility without equipment logical separation
        network = data.network
        for i, record in enumerate(network):
            s_utility = record.s_node.utility_no
            e_utility = record.e_node.utility_no
            
            # Skip if either node is equipment logical (valid separator)
            s_is_equipment_logical = (record.s_node.data_code == 15000 and record.s_node.nwo_type_no == 1)
            e_is_equipment_logical = (record.e_node.data_code == 15000 and record.e_node.nwo_type_no == 1)
            
            if s_is_equipment_logical or e_is_equipment_logical:
                continue
            
            # Skip if either node is special (PoC, forced)
            if record.s_node.nwo_type_no in [2, 3] or record.e_node.nwo_type_no in [2, 3]:
                continue
            
            # Invalid direct transition between regular nodes
            if s_utility and e_utility and s_utility != e_utility:
                errors.append(self._create_validation_error(
                    run_id,
                    data.execution_id,
                    'PATH_UTY_003',
                    Severity.CRITICAL,
                    ErrorType.INVALID_TRANSITION,
                    ObjectType.LINK,
                    record.link.id,
                    error_message=f'Invalid direct utility transition from {s_utility} to {e_utility} without equipment separation',
                    error_data=ContextHelper.to_json({
                        'path_id': data.path_id,
                        'link_id': record.link.id,
                        's_node_id': record.s_node.id,
                        'e_node_id': record.e_node.id,
                        'from_utility': s_utility,
                        'to_utility': e_utility,
                    })
                ))
        
        return errors
    
    def _create_validation_error(
        self,
        run_id: str,
        execution_id: str,
        error_code: str,
        severity: Severity,
        error_type: ErrorType,
        object_type: ObjectType,
        object_id: int,
        object_utility_no: Optional[int] = None,
        error_message: str = '',
        error_data: str = ''
    ) -> ValidationError:
        """Create a validation error object."""
        return ValidationError(
            run_id=run_id,
            execution_id=execution_id,
            error_code=error_code,
            severity=severity,
            error_type=error_type,
            object_type=object_type,
            object_id=object_id,
            object_utility_no=object_utility_no,
            error_message=error_message,
            error_data=error_data
        )
```
This focused implementation ensures **segment consistency** with three clear rules:

## Key Features:

1. **Well-Defined Segments**: Every segment must have a consistent utility throughout
2. **Equipment PoC Priority**: When equipment PoCs are present, they define the segment utility
3. **Proper Separation**: Utility changes only allowed at equipment logical node boundaries

## What This Catches:

- `AAAAAAA_BBBBBBB_CCCCCC` ✅ Valid (proper separation)
- `AAA?AAA_BBBBBBB` ❌ Missing utility in segment A
- `AAABBBAAAA` ❌ Invalid direct transition without equipment separation
- `AAA_AAA_BBB` ✅ Valid (equipment logical nodes separate properly)

## Optimization Benefits:

- **Single pass** through network data
- **No database calls** during validation
- **Focused validation** - only checks what matters for consistency
- **Clear error context** with segment boundaries

The algorithm efficiently identifies segments, determines expected utilities (with equipment PoC priority), and validates consistency within each segment while ensuring transitions only occur at proper boundaries.
