-- Validation tests for semiconductor fabrication path validation
-- Tests are grouped by functionality with specific error codes they can detect

-- PATH CONNECTIVITY TESTS (CRITICAL - Essential for semiconductor fabrication)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_CONN_001', 'Path Component Existence', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Validates all path components exist in database', 1, 'Detects: NOT_FOUND_NODE, NOT_FOUND_LINK, NOT_FOUND_NODES, NOT_FOUND_LINKS'),
('PATH_CONN_002', 'Path Structural Integrity', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Validates path structural completeness', 1, 'Detects: MISSING_START_NODE, MISSING_END_NODE, MISSING_POC, INVALID_NODE, INVALID_LINK'),
('PATH_CONN_003', 'Path Flow Continuity', 'CONNECTIVITY', 'CRITICAL', 'LOGICAL', 'Validates continuous path from start to end', 1, 'Detects: DISCONNECTED, BROKEN_CONTINUITY, DISCONNECTED_LINK, CONNECTING_GAP'),
('PATH_CONN_004', 'Network Object Types', 'CONNECTIVITY', 'WARNING', 'COMPLIANCE', 'Validates network object type classifications', 1, 'Detects: MISSING_NWO_TYPE (virtual vs physical elements)'),

-- PATH UTILITY TESTS (ERROR - Important for process flow)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_UTY_001', 'Utility Assignment Validation', 'FLOW', 'ERROR', 'COMPLIANCE', 'Validates utility assignments on path components', 1, 'Detects: MISSING_UTILITY, INVALID_UTILITY, WRONG_UTILITY'),
('PATH_UTY_002', 'Utility Consistency Check', 'FLOW', 'ERROR', 'LOGICAL', 'Validates utility consistency along path', 1, 'Detects: UTILITY_MISMATCH, INVALID_TRANSITION'),
('PATH_UTY_003', 'Utility Flow Validation', 'FLOW', 'WARNING', 'LOGICAL', 'Validates utility flow characteristics', 1, 'Detects: FLOW_DIRECTION_ISSUE, BIDIRECTIONAL_ISSUE'),

-- PATH MATERIAL TESTS (ERROR - Affects process materials)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_MAT_001', 'Material Assignment Validation', 'MATERIAL', 'ERROR', 'COMPLIANCE', 'Validates material assignments on path components', 1, 'Detects: MISSING_MATERIAL, INVALID_MATERIAL, WRONG_MATERIAL'),
('PATH_MAT_002', 'Material Consistency Check', 'MATERIAL', 'ERROR', 'LOGICAL', 'Validates material consistency along path', 1, 'Detects: MATERIAL_MISMATCH, INCOMPATIBILITY'),

-- PATH FLOW TESTS (ERROR - Critical for process flow)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_FLOW_001', 'Flow Direction Validation', 'FLOW', 'ERROR', 'COMPLIANCE', 'Validates flow direction assignments', 1, 'Detects: MISSING_FLOW, INVALID_FLOW, WRONG_FLOW'),
('PATH_FLOW_002', 'Flow Consistency Check', 'FLOW', 'ERROR', 'LOGICAL', 'Validates flow consistency along path', 1, 'Detects: FLOW_MISMATCH, FLOW_DIRECTION_ISSUE'),

-- PATH DATA QUALITY TESTS (WARNING - Data integrity)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_QA_001', 'Component Identification', 'QA', 'WARNING', 'COMPLIANCE', 'Validates component identification data', 1, 'Detects: MISSING_GUID, MISSING_DATA_CODE, MISSING_MARKERS'),
('PATH_QA_002', 'Cost Data Validation', 'QA', 'ERROR', 'LOGICAL', 'Validates component cost information', 1, 'Detects: MISSING_COST, NEGATIVE_COST'),
('PATH_QA_003', 'Reference Data Validation', 'QA', 'WARNING', 'COMPLIANCE', 'Validates component reference data', 1, 'Detects: MISSING_REFERENCE, MISSING'),
('PATH_QA_004', 'Path Data Completeness', 'QA', 'CRITICAL', 'STRUCTURAL', 'Validates essential path data presence', 1, 'Detects: MISSING_PATH_DATA (missing nodes/links data)'),

-- PATH PERFORMANCE TESTS (WARNING - Performance characteristics)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_PERF_001', 'Path Metrics Validation', 'QA', 'WARNING', 'PERFORMANCE', 'Validates path length and complexity metrics', 1, 'Detects: UNUSUAL_LENGTH, INVALID_LENGTH, HIGH_COMPLEXITY'),
('PATH_PERF_002', 'Path Topology Analysis', 'QA', 'WARNING', 'LOGICAL', 'Analyzes path topological characteristics', 1, 'Detects: UNUSUAL_TOPOLOGY, REDUNDANT_NODES, CIRCULAR_LOOP_DETECTED'),
('PATH_PERF_003', 'Component Usage Analysis', 'QA', 'WARNING', 'PERFORMANCE', 'Analyzes component utilization patterns', 1, 'Detects: NOT_USED, NOT_USED_POC, USAGE_STATUS'),

-- PATH SCENARIO TESTS (varies - Context-specific validation)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_SCEN_001', 'Contamination Risk Assessment', 'SCENARIO', 'ERROR', 'LOGICAL', 'Assesses cross-contamination risks in path', 1, 'Detects: CROSS_CONTAMINATION, INCOMPATIBILITY'),
('PATH_SCEN_002', 'Process Flow Validation', 'SCENARIO', 'WARNING', 'LOGICAL', 'Validates process-specific flow requirements', 1, 'Detects: CIRCULAR_LOOP_DETECTED, INVALID_TRANSITION'),

-- PATH NODE SPECIFIC TESTS (ERROR/CRITICAL - Node-level validation)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_NODE_001', 'Node Configuration Validation', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Validates individual node configurations', 1, 'Detects: INVALID_NODE (nodes not in path), INVALID_S_NODE, INVALID_E_NODE'),
('PATH_NODE_002', 'Node Data Completeness', 'QA', 'WARNING', 'COMPLIANCE', 'Validates node data completeness', 1, 'Detects: MISSING_DATA_CODE, MISSING_UTILITY, MISSING_MARKERS'),

-- PATH LINK SPECIFIC TESTS (ERROR/CRITICAL - Link-level validation)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_LINK_001', 'Link Configuration Validation', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Validates individual link configurations', 1, 'Detects: INVALID_LINK (links not in path), MISSING_START_NODE, MISSING_END_NODE'),
('PATH_LINK_002', 'Link Data Completeness', 'QA', 'WARNING', 'COMPLIANCE', 'Validates link data completeness', 1, 'Detects: MISSING_MATERIAL, MISSING_FLOW, MISSING_UTILITY'),

-- PATH FABRICATION SPECIFIC TESTS (semiconductor industry specific)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_FAB_001', 'Fabrication Phase Validation', 'QA', 'WARNING', 'COMPLIANCE', 'Validates fabrication phase assignments', 1, 'Detects: MISSING phase_no, INVALID phase transitions'),
('PATH_FAB_002', 'Model Consistency Check', 'QA', 'WARNING', 'LOGICAL', 'Validates model consistency across path', 1, 'Detects: model_no mismatches, INVALID model assignments'),
('PATH_FAB_003', 'E2E Group Validation', 'QA', 'WARNING', 'LOGICAL', 'Validates end-to-end group assignments', 1, 'Detects: e2e_group_no inconsistencies'),

-- PATH CRITICAL VALIDATION TESTS (CRITICAL - Must pass for valid path)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_CRIT_001', 'Critical Path Validation', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Overall critical path validation', 1, 'Comprehensive critical validation: connectivity, existence, continuity'),
('PATH_CRIT_002', 'Path Execution Validation', 'QA', 'CRITICAL', 'STRUCTURAL', 'Validates path execution data integrity', 1, 'Detects: MISSING_PATH_DATA, execution data corruption'),

-- PATH GENERIC VALIDATION TESTS (flexible validation patterns)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_GEN_001', 'Generic Data Validation', 'QA', 'WARNING', 'COMPLIANCE', 'Generic validation for missing or invalid data', 1, 'Detects: MISSING, INVALID, WRONG (generic patterns)'),
('PATH_GEN_002', 'Generic Mismatch Detection', 'QA', 'ERROR', 'LOGICAL', 'Generic validation for data mismatches', 1, 'Detects: MISMATCH, UNUSUAL (generic inconsistencies)'),
('PATH_GEN_003', 'Configuration Change Validation', 'QA', 'ERROR', 'LOGICAL', 'Validates configuration changes and updates', 1, 'Detects: INVALID_CHANGE, configuration inconsistencies'),

-- PATH LOOPBACK TESTS (special case for loopback paths)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_LOOP_001', 'Loopback Path Validation', 'CONNECTIVITY', 'WARNING', 'LOGICAL', 'Validates loopback path characteristics', 1, 'Detects: object_is_loopback flag validation, loop consistency'),

-- PATH COVERAGE TESTS (performance and completeness)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('PATH_COV_001', 'Path Coverage Validation', 'QA', 'WARNING', 'PERFORMANCE', 'Validates path coverage metrics', 1, 'Detects: low coverage, coverage calculation errors'),
('PATH_COV_002', 'Network Coverage Analysis', 'QA', 'WARNING', 'PERFORMANCE', 'Analyzes network coverage patterns', 1, 'Detects: coverage gaps, unused network components');