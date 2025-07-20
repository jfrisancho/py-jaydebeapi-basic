CREATE TABLE nw_nodes (
    id BIGINT PRIMARY KEY NOT NULL,
    guid VARCHAR(64) NOT NULL,
    
    fab_no TINYINT NOT NULL,
    model_no TINYINT NOT NULL,
    phase_no TINYINT NOT NULL,
    data_code INTEGER NOT NULL,
    utility_category_no TINYINT,
    utility_no INTEGER,
    item_no INTEGER,
    e2e_group_no INTEGER NOT NULL,
    e2e_header_code VARCHAR(64),
    e2e_relating_code VARCHAR(64),
    line_code VARCHAR(256),
    system_line_code VARCHAR(256),
    tapping_valve_code VARCHAR(256),
    markers VARCHAR(128),
    
    nwo_type_no TINYINT NOT NULL
);

CREATE TABLE nw_links (
    id BIGINT PRIMARY KEY NOT NULL,
    guid VARCHAR(64) NOT NULL,
    
    s_node_id BIGINT REFERENCES nw_nodes(id) NOT NULL,
    e_node_id BIGINT REFERENCES nw_nodes(id) NOT NULL,
    
    bidirected CHAR(1) DEFAULT Y NOT NULL,
    cost DOUBLE DEFAULT 0.0 NOT NULL,
    
    nwo_type_no TINYINT NOT NULL
)

CREATE INDEX idx_uk_nw_links_guid ON nw_links(guid);
CREATE INDEX idx_nw_links_start_node ON nw_links(start_node_id);
CREATE INDEX idx_nw_links_end_node ON nw_links(end_node_id);
CREATE INDEX idx_nw_links_start_node_end_node ON nw_links(start_node_id, end_node_id);
CREATE INDEX idx_nw_links_nw_object_type ON nw_links(nwo_type_no);

CREATE TABLE tb_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    digest VARCHAR(64) NOT NULL,
    
    algorithm VARCHAR(64) NOT NULL,
    
    s_node_id BIGINT NOT NULL,
    e_node_id BIGINT NOT NULL,

    cost DOUBLE,
    
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE UNIQUE INDEX idx_uk_paths_digest ON tb_paths(digest);
CREATE INDEX idx_paths_start_node ON tb_paths(s_node_id);
CREATE INDEX idx_paths_end_node ON tb_paths(e_node_id);

CREATE TABLE tb_path_links (
    path_id INTEGER REFERENCES tb_paths(id) NOT NULL,
    seq INTEGER NOT NULL,
    
    link_id BIGINT NOT NULL,
    length DOUBLE,
    
    s_node_id BIGINT,
    s_node_data_code INTEGER,
    s_node_utility_no INTEGER,
    
    e_node_id BIGINT,
    e_node_data_code INTEGER,
    e_node_utility_no INTEGER,

    group_no INTEGER,
    sub_group_no INTEGER,
    
    is_reverse BIT(1) NOT NULL,
    node_flag CHAR(1),
    
    CONSTRAINT tb_path_links_pk PRIMARY KEY (path_id, seq)
);

CREATE INDEX idx_path_links_link ON tb_path_links(link_id);
CREATE INDEX idx_path_links_path ON tb_path_links(path_id);

-- Toolsets: Simple with unique code
CREATE TABLE tb_toolsets (
    code VARCHAR(64) PRIMARY KEY,       -- Unique toolset code

    fab_no INTEGER NOT NULL,
    model_no INTEGER NOT NULL,          -- Represents the data model type (BIM, 5D)
    phase_no INTEGER NOT NULL,          -- Store as A, B or P1 and P2 (system nomenclature)
    e2e_group_no INTEGER NOT NULL,
    
    -- Toolset metadata
    description VARCHAR(512),           -- Optional description not used in the system

    is_active BIT(1) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_toolsets_fab ON tb_toolsets (fab_no);
CREATE INDEX idx_toolsets_model ON tb_toolsets (model_no);
CREATE INDEX idx_toolsets_phase ON tb_toolsets (phase_no);
CREATE INDEX idx_toolsets_e2e_group ON tb_toolsets (e2e_group_no);
CREATE INDEX idx_toolsets_fab_model ON tb_toolsets (fab_no, model_no);
CREATE INDEX idx_toolsets_fab_phase ON tb_toolsets (fab_no, phase_no);
CREATE INDEX idx_toolsets_fab_model_phase ON tb_toolsets (fab, model_no, phase_no);

-- Equipment: Simple FK to toolset code
CREATE TABLE tb_equipments (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    toolset VARCHAR(64) REFERENCES tb_toolsets(code) NOT NULL,

    guid VARCHAR(64) UNIQUE NOT NULL,
    node_id INTEGER NOT NULL,           -- Virtual equipment node
    data_code INTEGER NOT NULL,
    category_no INTEGER NOT NULL,
    vertices INTEGER NOT NULL,

    kind VARCHAR(32),                   -- PRODUCTION, PROCESSING, SUPPLY, etc.
    
    name VARCHAR(128),                  -- Optional name
    description VARCHAR(512),           -- Optional description

    is_active BIT(1) NOT NULL,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_equipments_toolset ON tb_equipments (toolset);
CREATE INDEX idx_equipments_node ON tb_equipments (node_id);

-- Equipment PoCs: Same as before
CREATE TABLE tb_equipment_pocs (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    equipment_id INTEGER REFERENCES tb_equipment(id) ON DELETE CASCADE NOT NULL,

    node_id INTEGER NOT NULL,           -- Actual network node ID
    
    markers VARCHAR(128)        -- Identifies PoC labels and associated metadata changes for this element
    reference VARCHAR(8)        -- Identifies the first formatter element of the markers
    utility_no INTEGER,         -- N2, CDA, PW, etc. - NULL if unused
    material_no INTEGER,  -- there are NULL for this iteration
    flow VARCHAR(8),            -- IN, OUT - NULL if unused (in this iteration is always NULL)
    
    is_used BIT(1) NOT NULL,
    is_loopback BIT(1) NOT NULL, -- If is there is a path connecting two or more PoCs in the same equipment.
    
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_eq_pocs_equipment ON tb_equipment_pocs(equipment_id);
CREATE UNIQUE INDEX idx_uk_eq_pocs_node ON tb_equipment_pocs(node_id);
CREATE INDEX idx_eq_pocs_equipment_poc_node ON tb_equipment_pocs(equipment_id, node_id);

CREATE TABLE tb_equipment_poc_connections (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,

    f_equipment_id INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,
    t_equipment_id   INTEGER NOT NULL REFERENCES tb_equipments(id) ON DELETE CASCADE,

    f_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,
    t_poc_id INTEGER NOT NULL REFERENCES tb_equipment_pocs(id) ON DELETE CASCADE,

    connection_type VARCHAR(16), -- Optional: STRAIGHT, BRANCHED, LOOPBACK, etc.

    is_active BIT(1) NOT NULL,  -- Mark whether the path is usable or blocked
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

CREATE INDEX idx_eq_poc_conn_from_eq_to_eq ON tb_equipment_poc_connections (f_equipment_id, t_equipment_id);
CREATE INDEX idx_eq_poc_conn_from_eq_poc ON tb_equipment_poc_connections (f_equipment_id, f_poc_id);
CREATE INDEX idx_eq_poc_conn_to_eq_poc ON tb_equipment_poc_connections (t_equipment_id, t_poc_id);

-- Runs: CLI execution metadata and coverage summary
CREATE TABLE tb_runs (
    id VARCHAR(45) PRIMARY KEY,
    
    approach VARCHAR(16) NOT NULL,      -- RANDOM, SCENARIO
    method VARCHAR(16) NOT NULL,        -- SIMPLE, STRATIFIED, PREDEFINED, SYNTHETIC, FILE
    
    -- Random-specific fields
    coverage_target FLOAT NOT NULL,     -- Only relevant for RANDOM approach
    fab_no INTEGER,                     -- Building identifier number (M15, M15X, M16) - NULL for SCENARIO
    phase_no INTEGER,                   -- Phase identifier number - NULL for SCENARIO
    model_no INTEGER,                   -- Data model type identifier number - NULL for SCENARIO
    
    e2e_group_nos VARCHAR(8000),
    
    -- Scenario-specific fields
    scenario_code VARCHAR(128),         -- Scenario code (PREXXXXXXX, SYNXXXXXXX) - NULL for RANDOM
    scenario_file VARCHAR(512),         -- Scenario file path - NULL for RANDOM

    tag VARCHAR(128) NOT NULL,          -- Auto-generated tag
    status VARCHAR(20) NOT NULL,        -- RUNNING, DONE, FAILED
    
    total_coverage FLOAT,
    total_nodes INTEGER,
    total_links INTEGER,
    
    -- Metadata
    run_at TIMESTAMP DEFAULT now() NOT NULL NOT NULL,
    ended_at TIMESTAMP
);

-- 6. Path Definitions: Enhanced with scenario support
CREATE TABLE tb_path_definitions (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    digest VARCHAR(64) NOT NULL,

    -- Path classification
    source_type VARCHAR(16),  -- RANDOM, SCENARIO
    scope VARCHAR(32),  -- CONNECTIVITY, FLOW, MATERIAL
    
    s_node_id BIGINT,
    e_node_id BIGINT,
    
    filter_fab_no INTEGER,  -- NULL for scenarios
    filter_model_no INTEGER,
    filter_phase_no INTEGER,
    filter_toolset_no INTEGER,

    filter_e2e_group_nos VARCHAR(8000),
    filter_category_nos VARCHAR(128),
    filter_utilitie_nos VARCHAR(900),
    filter_references VARCHAR(128), 
    
    target_data_codes VARCHAR(128),
    forbidden_node_ids VARCHAR(128),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT now() NOT NULL,
);

-- 7. Attempt Paths: Random sampling attempts
CREATE TABLE tb_attempt_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    path_definition_id INTEGER REFERENCES tb_path_definitions(id) ON DELETE CASCADE NOT NULL,

    status VARCHAR(16) NOT NULL,  -- FOUND, NOT_FOUND, ...
    path_digest VARCHAR(64),

    picked_at TIMESTAMP DEFAULT now() NOT NULL,
    notes VARCHAR(512)
);

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
    --test_type VARCHAR(32),  -- STRUCTURAL, LOGICAL, PERFORMANCE, COMPLIANCE
    reason VARCHAR(128),
    
    -- Applicability
  --  applies_to_random BIT(1) NOT NULL,
  --  applies_to_scenario BIT(1) NOT NULL,
    
    is_active BIT(1) NOT NULL,
    description VARCHAR(512)
);

CREATE TABLE tb_validation_outcomes (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    validation_test_id INTEGER REFERENCES tb_validation_tests(id) ON DELETE CASCADE NOT NULL,
    
    tag_type VARCHAR(16) NOT NULL,      -- QA, RISK, INS, CRIT, UTY, CAT, DAT, FAB, SCENARIO
    tag_code VARCHAR(48) NOT NULL,
    tag VARCHAR(64),
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

-- 13. Run Summaries: Enhanced aggregated metrics
CREATE TABLE tb_run_summaries (
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    
    -- Basic metrics
    total_attempts INTEGER NOT NULL,
    total_paths_found INTEGER NOT NULL,
    unique_paths INTEGER NOT NULL,
    
    -- Approach-specific metrics
    total_scenario_tests INTEGER NOT NULL DEFAULT 0,
    scenario_success_rate NUMERIC(5,2),
    
    -- Quality metrics
    total_errors INTEGER DEFAULT 0 NOT NULL,
    total_reviews INTEGER DEFAULT 0 NOT NULL,
    critical_errors INTEGER DEFAULT 0 NOT NULL,
    
    -- Coverage metrics (for RANDOM approach)
    target_coverage FLOAT,
    achieved_coverage FLOAT,
    coverage_efficiency FLOAT,  -- achieved/target ratio
    
    -- Performance metrics
    total_nodes INTEGER NOT NULL,
    total_links INTEGER NOT NULL,
    
    avg_path_nodes NUMERIC(10,2),
    avg_path_links NUMERIC(10,2),
    avg_path_length NUMERIC(15,3),
    
    -- Success metrics
    success_rate NUMERIC(5,2),
    completion_status VARCHAR(20),  -- COMPLETED, PARTIAL, FAILED
    
    -- Timing
    execution_time_s NUMERIC(10,2),
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    
    summarized_at TIMESTAMP DEFAULT now() NOT NULL,
    
    CONSTRAINT tb_run_coverage_summary_px PRIMARY KEY (run_id)
);

-- Indexes for performance
CREATE INDEX idx_runs_approach_status ON tb_runs(approach, status);
CREATE INDEX idx_runs_fab_date ON tb_runs(fab, date);
CREATE INDEX idx_runs_scenario ON tb_runs(scenario_code, scenario_type);

CREATE TABLE tb_run_coverage_summary (
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    
    total_nodes_in_scope INTEGER NOT NULL,
    total_links_in_scope INTEGER NOT NULL,
    
    covered_nodes INTEGER NOT NULL,
    covered_links INTEGER NOT NULL,
    
    node_coverage_pct NUMERIC(10,2) NOT NULL,
    link_coverage_pct NUMERIC(10,2) NOT NULL,
    overall_coverage_pct NUMERIC(10,2) NOT NULL,
    
    unique_paths_count INTEGER NOT NULL,
    
    scope_filters CLOB,
    
    created_at TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT tb_run_coverage_summary_px PRIMARY KEY (run_id)
)

CREATE TABLE tb_run_covered_nodes (
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    node_id BIGINT NOT NULL,

    covered_at TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT tb_run_covered_nodes_pk PRIMARY KEY (run_id, node_id)
)

INDEX idx_covered_nodes_run ON tb_run_covered_nodes(run_id);
INDEX idx_covered_nodes_node ON tb_run_covered_nodes(node_id);
 
CREATE TABLE tb_run_covered_links (
    run_id VARCHAR(45) REFERENCES tb_runs(id) ON DELETE CASCADE NOT NULL,
    link_id BIGINT NOT NULL,
    
    covered_at TIMESTAMP DEFAULT now() NOT NULL,
    CONSTRAINT tb_run_covered_links_pk PRIMARY KEY (run_id, link_id)
)

CREATE INDEX idx_covered_links_run ON tb_run_covered_links(run_id);
CREATE INDEX idx_covered_links_link ON tb_run_covered_links(link_id);