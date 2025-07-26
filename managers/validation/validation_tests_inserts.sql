-- Validation Tests Inserts
-- These tests define the validation framework for path connectivity and utilities

-- 1. CONNECTIVITY VALIDATION TESTS (CRITICAL - breaks connectivity)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_CONN_001', 'Path Data Completeness', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Path must have nodes and links to be valid', 1, 'Validates that path has minimum required nodes and links data'),
('PATH_CONN_002', 'Node Existence Validation', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'All path nodes must exist in sampling universe', 1, 'Validates that all nodes in path exist and are accessible'),
('PATH_CONN_003', 'Link Existence Validation', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'All path links must exist in sampling universe', 1, 'Validates that all links in path exist and are accessible'),
('PATH_CONN_004', 'Path Endpoints Validation', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Path must have valid start and end nodes', 1, 'Validates that path has proper start and end node definitions'),
('PATH_CONN_005', 'Node-Link Continuity', 'CONNECTIVITY', 'CRITICAL', 'LOGICAL', 'Links must properly connect nodes in sequence', 1, 'Validates that links create continuous path between nodes'),
('PATH_CONN_006', 'Disconnected Elements', 'CONNECTIVITY', 'ERROR', 'LOGICAL', 'Path should not contain disconnected elements', 1, 'Identifies disconnected nodes or links that break path flow'),
('PATH_CONN_007', 'Bidirectional Consistency', 'CONNECTIVITY', 'ERROR', 'LOGICAL', 'Bidirectional links must be consistent', 1, 'Validates bidirectional link consistency and flow direction');

-- 2. UTILITY CONSISTENCY VALIDATION TESTS (HIGH - affects functionality)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_UTY_001', 'Utility Code Presence', 'UTILITY', 'ERROR', 'COMPLIANCE', 'Nodes should have utility codes where required', 1, 'Validates presence of utility codes on non-virtual nodes'),
('PATH_UTY_002', 'Utility Consistency', 'UTILITY', 'ERROR', 'LOGICAL', 'Connected nodes should have compatible utilities', 1, 'Validates utility compatibility between connected nodes'),
('PATH_UTY_003', 'Utility Transitions', 'UTILITY', 'WARNING', 'LOGICAL', 'Utility changes should be valid transitions', 1, 'Validates that utility changes follow allowed transition rules'),
('PATH_UTY_004', 'Utility Scope Compliance', 'UTILITY', 'WARNING', 'COMPLIANCE', 'Path utilities should match defined scope', 1, 'Validates that path utilities are within expected scope');

-- 3. POC CONFIGURATION VALIDATION TESTS (MEDIUM - configuration issues)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_POC_001', 'PoC Reference Validation', 'QA', 'WARNING', 'COMPLIANCE', 'Path should reference valid PoC configurations', 1, 'Validates PoC reference integrity and accessibility'),
('PATH_POC_002', 'PoC Usage Status', 'QA', 'WARNING', 'COMPLIANCE', 'PoC configurations should be properly utilized', 1, 'Identifies unused or improperly referenced PoC configurations');

-- 4. STRUCTURAL VALIDATION TESTS (MEDIUM - structural anomalies)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_STR_001', 'Path Length Validation', 'FLOW', 'WARNING', 'PERFORMANCE', 'Path length should be within reasonable bounds', 1, 'Validates path length against expected parameters'),
('PATH_STR_002', 'Node Redundancy Check', 'FLOW', 'WARNING', 'PERFORMANCE', 'Path should not contain redundant nodes', 1, 'Identifies potentially redundant nodes in path'),
('PATH_STR_003', 'Circular Loop Detection', 'FLOW', 'ERROR', 'LOGICAL', 'Path should not contain circular loops', 1, 'Detects circular loops that could cause routing issues'),
('PATH_STR_004', 'Path Complexity Analysis', 'FLOW', 'INFO', 'PERFORMANCE', 'Path complexity should be monitored', 1, 'Analyzes path complexity metrics for optimization'),
('PATH_STR_005', 'Virtual Node Validation', 'FLOW', 'INFO', 'STRUCTURAL', 'Virtual nodes should be properly configured', 1, 'Validates virtual node configuration and necessity');

-- 5. PERFORMANCE VALIDATION TESTS (LOW-MEDIUM - optimization opportunities)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_PER_001', 'Cost Validation', 'FLOW', 'WARNING', 'PERFORMANCE', 'Path costs should be realistic and positive', 1, 'Validates path cost calculations and identifies negative costs'),
('PATH_PER_002', 'Material Consistency', 'MATERIAL', 'WARNING', 'COMPLIANCE', 'Path materials should be consistent', 1, 'Validates material consistency along path segments'),
('PATH_PER_003', 'Flow Direction Analysis', 'FLOW', 'WARNING', 'LOGICAL', 'Flow directions should be consistent', 1, 'Analyzes flow direction consistency and identifies conflicts'),
('PATH_PER_004', 'Data Code Validation', 'QA', 'INFO', 'COMPLIANCE', 'Data codes should be present and valid', 1, 'Validates data code presence and validity'),
('PATH_PER_005', 'Path Markers Analysis', 'QA', 'INFO', 'COMPLIANCE', 'Path markers should be properly configured', 1, 'Analyzes path marker configuration and usage');

-- 6. SCENARIO-SPECIFIC VALIDATION TESTS (VARIABLE - context dependent)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES 
('PATH_SCN_001', 'Scenario Compatibility', 'SCENARIO', 'ERROR', 'COMPLIANCE', 'Path should be compatible with scenario requirements', 1, 'Validates path compatibility with specific scenario constraints'),
('PATH_SCN_002', 'Cross-Contamination Check', 'SCENARIO', 'CRITICAL', 'COMPLIANCE', 'Path should prevent cross-contamination in fab', 1, 'Critical validation for semiconductor fab contamination prevention');