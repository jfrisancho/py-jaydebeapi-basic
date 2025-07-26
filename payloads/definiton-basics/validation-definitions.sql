CREATE TABLE tb_path_executions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_id INTEGER REFERENCES tb_paths(id) ON DELETE CASCADE NOT NULL,
    
    execution_status VARCHAR(16) NOT NULL,
    execution_time_ms INTEGER,
   
    -- Path metrics
    node_count INTEGER DEFAULT 0 NOT NULL,
    link_count INTEGER NOT NULL,
    coverage FLOAT NOT NULL,
    cost DOUBLE NOT NULL,
    length_mm NUMERIC(15,3) NOT NULL,

    -- Path data
    data_codes_scope CLOB,
    utilities_scope CLOB,  -- String list of utility codes
    references_scope CLOB,
    path_context CLOB,  -- String with nodes/links sequence

    validation_passed BIT(1),
    validation_errors CLOB,

    executed_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_path_execution_run ON tb_path_executions(run_id);
CREATE INDEX idx_path_execution_path ON  tb_path_executions(path_id);
CREATE UNIQUE INDEX idx_uk_path_execution_run_path ON tb_path_executions(run_id, path_id);


-- 9. Path Tags: Enhanced with source tracking
CREATE TABLE tb_path_tags (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,
    path_digest VARCHAR(64),

    tag_type VARCHAR(16) NOT NULL,  -- QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO
    tag_code VARCHAR(48) NOT NULL,
    tag VARCHAR(64),
    
    -- Tag metadata
    source VARCHAR(20),  -- SYSTEM, USER, VALIDATION
    confidence FLOAT DEFAULT 1.0,  -- Confidence score for auto-generated tags
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    notes VARCHAR(512)
);

CREATE INDEX idx_path_tags_definition ON tb_path_tags (path_definition_id);
CREATE UNIQUE INDEX idx_path_tags_path_digest ON tb_path_tags (path_digest);
CREATE INDEX idx_path_tags_type ON tb_path_tags (tag_type, tag_code);

-- 10. Validation Tests: Enhanced validation framework
CREATE TABLE tb_validation_tests (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    code VARCHAR(32) UNIQUE NOT NULL,
    name VARCHAR(128) NOT NULL,

    scope VARCHAR(32) NOT NULL,  -- FLOW, CONNECTIVITY, MATERIAL, QA, SCENARIO
    severity VARCHAR(16) NOT NULL,  -- LOW, MEDIUM, HIGH, CRITICAL
    test_type VARCHAR(16) NOT NULL,  -- STRUCTURAL, LOGICAL, PERFORMANCE, COMPLIANCE
    reason VARCHAR(128),
    
    -- Applicability
  --  applies_to_random BIT(1) NOT NULL,
  --  applies_to_scenario BIT(1) NOT NULL,
    
    is_active BIT(1) NOT NULL,
    description VARCHAR(512)
);


-- 11. Validation Errors: Enhanced error tracking
CREATE TABLE tb_validation_errors (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_execution_id INTEGER REFERENCES tb_path_executions(id) ON DELETE CASCADE NOT NULL,
    validation_test_id REFERENCES tb_validation_tests(id) ON DELETE CASCADE NOT NULL,

    severity VARCHAR(16) NOT NULL,
    error_scope VARCHAR(64) NOT NULL,
    error_type VARCHAR(64) NOT NULL,
    
    -- Object references
    object_type VARCHAR(8) NOT NULL,
    object_id BIGINT NOT NULL,
    object_guid VARCHAR(64) NOT NULL,

    object_fab_no INTEGER,
    object_model_no INTEGER,
    object_phase_no INTEGER,
    object_data_code INTEGER,
    object_e2e_group_no INTEGER,
    object_markers VARCHAR(128),
    object_utility_no INTEGER,
    object_item_no INTEGER,
    object_type_no INTEGER,

    object_material_no INTEGER,
    object_flow VARCHAR(8),
    object_is_loopback BIT(1) NOT NULL,
    object_cost DOUBLE,

    -- Error details
    --error_message CLOB,
    --error_data CLOB,  -- String with additional error data
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    notes VARCHAR(512)
);
    
CREATE INDEX idx_validation_errors_run ON tb_validation_errors (run_id),
CREATE INDEX idx_validation_errors_severity ON tb_validation_errors (severity),
CREATE INDEX idx_validation_errors_type ON tb_validation_errors (error_type)

-- 12. Review Flags: Enhanced flagging system
CREATE TABLE tb_run_reviews (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    
    flag_type VARCHAR(32) NOT NULL,  -- MANUAL_REVIEW, CRITICAL_ERROR, PERFORMANCE, ANOMALY
    severity VARCHAR(16) NOT NULL,
    reason VARCHAR(256) NOT NULL,

    -- Object references
    object_type VARCHAR(8) NOT NULL,
    object_id BIGINT NOT NULL,
    object_guid VARCHAR(64) NOT NULL,

    object_fab_no INTEGER,
    object_model_no INTEGER,
    object_phase_no INTEGER,
    object_e2e_group_no INTEGER,
    object_data_code INTEGER,
    object_markers VARCHAR(128),
    object_utility_no INTEGER,
    object_item_no INTEGER,
    object_type_no INTEGER,

    object_material_no INTEGER,
    object_flow VARCHAR(8),
    object_is_loopback BIT(1) NOT NULL,
    object_cost DOUBLE,
    
    path_context CLOB,  -- String with path context
    
    -- Flag lifecycle
    status VARCHAR(20) DEFAULT 'OPEN',  -- OPEN, ACKNOWLEDGED, RESOLVED, DISMISSED
    assigned_to VARCHAR(64),
    resolved_at TIMESTAMP,
    resolution_notes CLOB,
    
    created_at TIMESTAMP DEFAULT now() NOT NULL
);
    
CREATE INDEX idx_run_reviews_run ON tb_run_reviews (run_id),
CREATE INDEX idx_run_reviews_flag_type ON tb_run_reviews (flag_type),
CREATE INDEX idx_run_reviews_severity ON tb_run_reviews (severity)