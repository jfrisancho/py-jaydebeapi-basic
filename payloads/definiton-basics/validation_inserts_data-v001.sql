-- ==========================================
-- VALIDATION TESTS - REVISED WITH PROPER LOGIC
-- ==========================================

-- 1. PATH CONNECTIVITY VALIDATION (Basic Structural)
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_CONN_001',
    'Path Connectivity Validation',
    'CONNECTIVITY',
    'CRITICAL',
    'STRUCTURAL',
    'Ensure complete path exists from start to end without dead ends or broken links',
    1,
    'Validates that a complete traversable path exists from source to destination with proper link continuity and no dead ends'
);

-- 2. PATH UTILITY VALIDATION (Data Quality)
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_UTY_001',
    'Path Utility Data Validation',
    'MATERIAL',
    'ERROR',
    'LOGICAL',
    'Validate utility data integrity and transition patterns along complete paths',
    1,
    'Validates utility assignments and transition logic along paths, ensuring proper utility continuity and detecting anomalous patterns'
);

-- 3. NODE CRITICAL DATA VALIDATION
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'NODE_DATA_001',
    'Node Critical Data Validation',
    'QA',
    'ERROR',
    'COMPLIANCE',
    'Validate all critical node attributes required for path execution',
    1,
    'Validates presence and validity of critical node data including GUID, data_code, e2e_group_no, and other essential attributes'
);

-- 4. PATH COMPLETENESS VALIDATION
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_COMP_001',
    'Path Completeness Validation',
    'FLOW',
    'CRITICAL',
    'STRUCTURAL',
    'Ensure paths have proper start and end points with no dead ends',
    1,
    'Validates that paths are complete from beginning to end with proper termination points and no incomplete segments'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR PATH CONNECTIVITY
-- ==========================================

-- Path broken continuity
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'PATH',
    'CRIT',
    'BROKEN_CONTINUITY',
    'CRIT_PATH_BROKEN_CONTINUITY'
);

-- Missing links between nodes
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'LINK',
    'QA',
    'MISSING',
    'QA_LINK_MISSING'
);

-- Connection gaps in path
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'SEGMENT',
    'CRIT',
    'CONNECTING_GAP',
    'CRIT_SEGMENT_CONNECTING_GAP'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR PATH UTILITIES
-- ==========================================

-- Missing utility on nodes
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'QA',
    'MISSING_UTILITY',
    'QA_NODE_MISSING_UTILITY'
);

-- Invalid utility values
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'UTY',
    'INVALID_UTILITY',
    'UTY_NODE_INVALID_UTILITY'
);

-- Utility transition issues between nodes
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'SEGMENT',
    'UTY',
    'INVALID_TRANSITION',
    'UTY_SEGMENT_INVALID_TRANSITION'
);

-- Utility mismatch in path segments
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'SEGMENT',
    'UTY',
    'UTILITY_MISMATCH',
    'UTY_SEGMENT_UTILITY_MISMATCH'
);

-- Cross contamination between utilities
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'PATH',
    'CRIT',
    'CROSS_CONTAMINATION',
    'CRIT_PATH_CROSS_CONTAMINATION'
);

-- Unusual utility pattern sequences (A-A-A-B-A-A)
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'PATH',
    'RISK',
    'UNUSUAL_LENGTH',
    'RISK_PATH_UNUSUAL_LENGTH'
);

-- Wrong utility assignment for node type
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'UTY',
    'WRONG_UTILITY',
    'UTY_NODE_WRONG_UTILITY'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR NODE CRITICAL DATA
-- ==========================================

-- Missing GUID
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_GUID',
    'QA_NODE_MISSING_GUID'
);

-- Missing data_code
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_DATA_CODE',
    'QA_NODE_MISSING_DATA_CODE'
);

-- Missing e2e_group_no
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING',
    'QA_NODE_MISSING'
);

-- Missing utility reference
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_UTILITY',
    'QA_NODE_MISSING_UTILITY'
);

-- Missing material reference
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_MATERIAL',
    'QA_NODE_MISSING_MATERIAL'
);

-- Missing flow data
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_FLOW',
    'QA_NODE_MISSING_FLOW'
);

-- Missing cost data
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_COST',
    'QA_NODE_MISSING_COST'
);

-- Missing markers
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_MARKERS',
    'QA_NODE_MISSING_MARKERS'
);

-- Missing NWO type
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_NWO_TYPE',
    'QA_NODE_MISSING_NWO_TYPE'
);

-- Invalid reference data
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'NODE_DATA_001'),
    'NODE',
    'QA',
    'MISSING_REFERENCE',
    'QA_NODE_MISSING_REFERENCE'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR PATH COMPLETENESS
-- ==========================================

-- Path has dead end
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'PATH',
    'CRIT',
    'NOT_FOUND',
    'CRIT_PATH_NOT_FOUND'
);

-- Path incomplete - missing end point
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'PATH',
    'CRIT',
    'MISSING',
    'CRIT_PATH_MISSING'
);

-- Flow direction issues preventing completion
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'SEGMENT',
    'FLOW',
    'FLOW_DIRECTION_ISSUE',
    'FLOW_SEGMENT_FLOW_DIRECTION_ISSUE'
);

-- Bidirectional flow problems
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'SEGMENT',
    'FLOW',
    'BIDIRECTIONAL_ISSUE',
    'FLOW_SEGMENT_BIDIRECTIONAL_ISSUE'
);

-- Circular loop detected preventing completion
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'PATH',
    'RISK',
    'CIRCULAR_LOOP_DETECTED',
    'RISK_PATH_CIRCULAR_LOOP_DETECTED'
);

-- Path requires review due to complexity
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_COMP_001'),
    'PATH',
    'INS',
    'NOT_FOUND_REVIEW',
    'INS_PATH_NOT_FOUND_REVIEW'
);
