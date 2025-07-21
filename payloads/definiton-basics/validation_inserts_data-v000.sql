-- ==========================================
-- VALIDATION TESTS
-- ==========================================

-- 1. PATH CONNECTIVITY VALIDATION (Basic Level)
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_CONN_001',
    'Path Connectivity Validation',
    'CONNECTIVITY',
    'CRITICAL',
    'STRUCTURAL',
    'Ensure valid path exists between start and end nodes',
    1,
    'Validates that a complete path exists from source to destination with proper link continuity between consecutive nodes'
);

-- 2. PATH UTILITY VALIDATION (Data Quality Level)
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_UTY_001',
    'Path Utility Data Quality Validation',
    'MATERIAL',
    'WARNING',
    'LOGICAL',
    'Identify utility data anomalies and inconsistent patterns along path',
    1,
    'Validates utility data integrity along paths, detecting null utilities, unusual transition patterns, and data consistency issues'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR PATH CONNECTIVITY
-- ==========================================

-- Broken Continuity Issues
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'PATH',
    'QA',
    'BROKEN_CONTINUITY',
    'QA_PATH_BROKEN_CONTINUITY'
);

-- Missing Links in Path
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'LINK',
    'QA',
    'MISSING',
    'QA_LINK_MISSING'
);

-- Invalid Transitions
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'NODE',
    'QA',
    'INVALID_TRANSITION',
    'QA_NODE_INVALID_TRANSITION'
);

-- Connecting Gap Issues
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_CONN_001'),
    'PATH',
    'QA',
    'CONNECTING_GAP',
    'QA_PATH_CONNECTING_GAP'
);

-- ==========================================
-- VALIDATION OUTCOMES FOR PATH UTILITY
-- ==========================================

-- Missing Utility Data
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'UTY',
    'MISSING_UTILITY',
    'UTY_NODE_MISSING_UTILITY'
);

-- Invalid Utility Values
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'UTY',
    'INVALID_UTILITY',
    'UTY_NODE_INVALID_UTILITY'
);

-- Utility Mismatch in Path
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'PATH',
    'UTY',
    'UTILITY_MISMATCH',
    'UTY_PATH_UTILITY_MISMATCH'
);

-- Wrong Utility Assignment
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'UTY',
    'WRONG_UTILITY',
    'UTY_NODE_WRONG_UTILITY'
);

-- High Complexity Utility Pattern
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'PATH',
    'CRIT',
    'HIGH_COMPLEXITY',
    'CRIT_PATH_HIGH_COMPLEXITY'
);

-- Cross Contamination (Utility mixing)
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'PATH',
    'CRIT',
    'CROSS_CONTAMINATION',
    'CRIT_PATH_CROSS_CONTAMINATION'
);

-- Missing Reference Data
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_001'),
    'NODE',
    'QA',
    'MISSING_REFERENCE',
    'QA_NODE_MISSING_REFERENCE'
);

-- ==========================================
-- ADDITIONAL VALIDATION TEST FOR DETAILED UTILITY PATTERNS
-- ==========================================

-- 3. PATH UTILITY PATTERN ANALYSIS (Advanced Data Quality)
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, reason, is_active, description
) VALUES (
    'PATH_UTY_002',
    'Path Utility Pattern Analysis',
    'MATERIAL',
    'WARNING',
    'LOGICAL',
    'Detect specific utility transition patterns that indicate potential data quality issues',
    1,
    'Advanced analysis of utility sequences along paths to identify patterns like A-A-A-B-A-A or A-A-A-NULL-NULL that may indicate data entry errors or system issues'
);

-- Unusual Utility Transition Patterns
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_002'),
    'PATH',
    'RISK',
    'UNUSUAL_LENGTH',
    'RISK_PATH_UNUSUAL_LENGTH'
);

-- Redundant Node Pattern Detection
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_002'),
    'PATH',
    'RISK',
    'REDUNDANT_NODES',
    'RISK_PATH_REDUNDANT_NODES'
);

-- Invalid Utility Change Pattern
INSERT INTO tb_validation_outcomes (
    validation_test_id, object_type, tag_type, tag_code, tag
) VALUES (
    (SELECT id FROM tb_validation_tests WHERE code = 'PATH_UTY_002'),
    'PATH',
    'UTY',
    'INVALID_CHANGE',
    'UTY_PATH_INVALID_CHANGE'
);
