-- Validation tests for semiconductor fabrication path validation
-- Focus on connectivity and utility validation as requested

-- Connectivity Tests (CRITICAL - anything that breaks connectivity is critical)
INSERT INTO tb_validation_tests (code, name, scope, severity, test_type, reason, is_active, description) VALUES
('NOT_FOUND_NODE', 'Node Not Found', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Referenced node does not exist in database', 1, 'Validates that all path nodes exist in the network database'),
('NOT_FOUND_LINK', 'Link Not Found', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Referenced link does not exist in database', 1, 'Validates that all path links exist in the network database'),
('MISSING_START_END_NODE', 'Missing Start/End Node', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Link missing start or end node reference', 1, 'Ensures all links have valid start and end node references'),
('DISCONNECTED', 'Disconnected Component', 'CONNECTIVITY', 'CRITICAL', 'LOGICAL', 'Path component is not properly connected', 1, 'Detects nodes or links that break path continuity'),
('BROKEN_CONTINUITY', 'Broken Path Continuity', 'CONNECTIVITY', 'CRITICAL', 'LOGICAL', 'Path flow is interrupted', 1, 'Validates continuous flow from start to end of path'),
('DISCONNECTED_LINK', 'Disconnected Link', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Link is not connected to path nodes', 1, 'Ensures all links connect to valid path nodes'),

-- Utility Tests (ERROR level - important but may not break functionality)
('UTILITY_MISMATCH', 'Utility Mismatch', 'FLOW', 'ERROR', 'LOGICAL', 'Component utility does not match expected path utilities', 1, 'Validates utility consistency along path'),
('INVALID_UTILITY', 'Invalid Utility', 'FLOW', 'ERROR', 'COMPLIANCE', 'Component has invalid or undefined utility', 1, 'Checks for valid utility assignments on path components'),
('WRONG_UTILITY', 'Wrong Utility Type', 'FLOW', 'ERROR', 'LOGICAL', 'Component utility type is incorrect for path context', 1, 'Validates utility type compatibility within path'),
('MISSING_UTILITY', 'Missing Utility Assignment', 'FLOW', 'WARNING', 'COMPLIANCE', 'Component missing required utility assignment', 1, 'Detects components without utility assignments'),
('INVALID_TRANSITION', 'Invalid Utility Transition', 'FLOW', 'WARNING', 'LOGICAL', 'Invalid transition between different utilities', 1, 'Validates utility transitions along path sequence'),

-- Material Tests (ERROR level - affects flow characteristics)
('MATERIAL_MISMATCH', 'Material Mismatch', 'MATERIAL', 'ERROR', 'LOGICAL', 'Component material does not match expected path materials', 1, 'Validates material consistency along path'),
('INVALID_MATERIAL', 'Invalid Material', 'MATERIAL', 'ERROR', 'COMPLIANCE', 'Component has invalid or undefined material', 1, 'Checks for valid material assignments'),
('MISSING_MATERIAL', 'Missing Material Assignment', 'MATERIAL', 'WARNING', 'COMPLIANCE', 'Component missing required material assignment', 1, 'Detects components without material assignments'),
('WRONG_MATERIAL', 'Wrong Material Type', 'MATERIAL', 'ERROR', 'LOGICAL', 'Component material type is incorrect for path context', 1, 'Validates material compatibility within path'),

-- Flow Tests (ERROR level - affects process flow)
('FLOW_MISMATCH', 'Flow Direction Mismatch', 'FLOW', 'ERROR', 'LOGICAL', 'Component flow direction does not match expected path flow', 1, 'Validates flow direction consistency'),
('INVALID_FLOW', 'Invalid Flow Direction', 'FLOW', 'ERROR', 'COMPLIANCE', 'Component has invalid flow direction', 1, 'Checks for valid flow direction assignments'),
('MISSING_FLOW', 'Missing Flow Assignment', 'FLOW', 'WARNING', 'COMPLIANCE', 'Component missing required flow assignment', 1, 'Detects components without flow assignments'),
('FLOW_DIRECTION_ISSUE', 'Flow Direction Issue', 'FLOW', 'WARNING', 'LOGICAL', 'Potential flow direction inconsistency', 1, 'Identifies potential flow direction problems'),

-- Data Quality Tests (WARNING level - affects data integrity)
('MISSING_GUID', 'Missing GUID', 'QA', 'WARNING', 'COMPLIANCE', 'Component missing unique identifier', 1, 'Ensures all components have valid GUIDs'),
('MISSING_DATA_CODE', 'Missing Data Code', 'QA', 'WARNING', 'COMPLIANCE', 'Component missing data classification code', 1, 'Validates data code assignments'),
('MISSING_COST', 'Missing Cost Data', 'QA', 'WARNING', 'COMPLIANCE', 'Component missing cost information', 1, 'Detects components without cost data'),
('NEGATIVE_COST', 'Negative Cost Value', 'QA', 'ERROR', 'LOGICAL', 'Component has negative cost value', 1, 'Identifies invalid negative cost values'),
('MISSING_REFERENCE', 'Missing Reference Data', 'QA', 'WARNING', 'COMPLIANCE', 'Component missing reference information', 1, 'Validates reference data completeness'),

-- Performance Tests (WARNING level - affects performance metrics)
('UNUSUAL_LENGTH', 'Unusual Path Length', 'QA', 'WARNING', 'PERFORMANCE', 'Path length is unusually long or short', 1, 'Identifies paths with unusual length characteristics'),
('HIGH_COMPLEXITY', 'High Path Complexity', 'QA', 'WARNING', 'PERFORMANCE', 'Path has unusually high complexity', 1, 'Detects paths with high node/link ratios'),
('UNUSUAL_TOPOLOGY', 'Unusual Path Topology', 'QA', 'WARNING', 'LOGICAL', 'Path has unusual topological characteristics', 1, 'Identifies paths with unusual structural patterns'),
('REDUNDANT_NODES', 'Redundant Nodes Detected', 'QA', 'WARNING', 'PERFORMANCE', 'Path contains redundant or unnecessary nodes', 1, 'Detects potentially redundant path components'),

-- Scenario-Specific Tests (varies by scenario requirements)
('CIRCULAR_LOOP_DETECTED', 'Circular Loop Detected', 'SCENARIO', 'WARNING', 'LOGICAL', 'Path contains circular loop', 1, 'Detects circular paths that may cause issues'),
('CROSS_CONTAMINATION', 'Cross Contamination Risk', 'SCENARIO', 'ERROR', 'LOGICAL', 'Path poses cross-contamination risk', 1, 'Identifies potential contamination issues'),
('INCOMPATIBILITY', 'Component Incompatibility', 'SCENARIO', 'ERROR', 'LOGICAL', 'Components are incompatible in current context', 1, 'Detects incompatible component combinations'),

-- Missing Component Tests (CRITICAL - essential components missing)
('MISSING_PATH_DATA', 'Missing Path Data', 'QA', 'CRITICAL', 'STRUCTURAL', 'Path execution missing essential data', 1, 'Validates that path has required nodes and links data'),
('NOT_FOUND_NODES', 'Multiple Nodes Not Found', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Multiple referenced nodes do not exist', 1, 'Validates existence of multiple path nodes'),
('NOT_FOUND_LINKS', 'Multiple Links Not Found', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Multiple referenced links do not exist', 1, 'Validates existence of multiple path links'),

-- Coverage and Usage Tests (WARNING level - affects coverage metrics)
('NOT_USED', 'Component Not Used', 'QA', 'WARNING', 'PERFORMANCE', 'Component exists but is not utilized in path', 1, 'Identifies unused components in path context'),
('NOT_USED_POC', 'POC Not Used', 'QA', 'WARNING', 'PERFORMANCE', 'Point of connection not utilized', 1, 'Detects unused points of connection'),
('USAGE_STATUS', 'Usage Status Issue', 'QA', 'WARNING', 'LOGICAL', 'Component usage status is inconsistent', 1, 'Validates component usage status'),

-- Advanced Validation Tests (varies by context)
('BIDIRECTIONAL_ISSUE', 'Bidirectional Flow Issue', 'FLOW', 'WARNING', 'LOGICAL', 'Issues with bidirectional flow handling', 1, 'Validates bidirectional flow configurations'),
('CONNECTING_GAP', 'Connection Gap Detected', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Gap detected in path connections', 1, 'Identifies gaps in path connectivity'),
('INVALID_CHANGE', 'Invalid Configuration Change', 'QA', 'ERROR', 'LOGICAL', 'Invalid change in component configuration', 1, 'Detects invalid configuration changes'),
('INVALID_LENGTH', 'Invalid Length Value', 'QA', 'ERROR', 'COMPLIANCE', 'Component has invalid length value', 1, 'Validates component length values'),

-- Node-Specific Tests
('INVALID_NODE', 'Invalid Node Configuration', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Node has invalid configuration', 1, 'Validates node configuration parameters'),
('INVALID_S_NODE', 'Invalid Start Node', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Path start node is invalid', 1, 'Validates path start node configuration'),
('INVALID_E_NODE', 'Invalid End Node', 'CONNECTIVITY', 'CRITICAL', 'STRUCTURAL', 'Path end node is invalid', 1, 'Validates path end node configuration'),

-- Link-Specific Tests
('INVALID_LINK', 'Invalid Link Configuration', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Link has invalid configuration', 1, 'Validates link configuration parameters'),

-- Fabrication-Specific Tests (for semiconductor context)
('MISSING_NWO_TYPE', 'Missing Network Object Type', 'QA', 'WARNING', 'COMPLIANCE', 'Network object missing type classification', 1, 'Validates network object type assignments'),
('MISSING_MARKERS', 'Missing Component Markers', 'QA', 'WARNING', 'COMPLIANCE', 'Component missing required markers', 1, 'Validates component marker assignments'),
('MISSING_POC', 'Missing Point of Connection', 'CONNECTIVITY', 'ERROR', 'STRUCTURAL', 'Required point of connection is missing', 1, 'Validates required connection points'),

-- Generic Validation Tests
('MISSING', 'Missing Required Data', 'QA', 'WARNING', 'COMPLIANCE', 'Required data field is missing', 1, 'Generic test for missing required data'),
('INVALID', 'Invalid Configuration', 'QA', 'ERROR', 'COMPLIANCE', 'Component configuration is invalid', 1, 'Generic test for invalid configurations'),
('WRONG', 'Incorrect Value', 'QA', 'ERROR', 'LOGICAL', 'Component has incorrect value', 1, 'Generic test for incorrect values'),
('UNUSUAL', 'Unusual Characteristic', 'QA', 'WARNING', 'PERFORMANCE', 'Component has unusual characteristics', 1, 'Generic test for unusual patterns'),
('MISMATCH', 'Data Mismatch', 'QA', 'ERROR', 'LOGICAL', 'Data inconsistency detected', 1, 'Generic test for data mismatches');