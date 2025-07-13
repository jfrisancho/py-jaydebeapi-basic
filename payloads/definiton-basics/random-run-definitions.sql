-- 1. CORE RUNS TABLE
CREATE TABLE tb_runs (
    id              VARCHAR(45)     PRIMARY KEY,
    approach        VARCHAR(16)     NOT NULL  /* 'RANDOM' or 'SCENARIO' */,
    method          VARCHAR(16)     NOT NULL,  /* e.g. 'SIMPLE','STRATIFIED' */
    tag             VARCHAR(128)    NOT NULL,
    status          VARCHAR(20)     NOT NULL,  /* 'RUNNING','DONE','FAILED' */
    total_coverage  FLOAT,
    total_nodes     INTEGER,
    total_links     INTEGER,
    run_at          TIMESTAMP       NOT NULL DEFAULT now(),
    ended_at        TIMESTAMP
);

-- 2. RANDOM-RUN PARAMETERS
CREATE TABLE tb_run_random_params (
    id               VARCHAR(45)    PRIMARY KEY
                       REFERENCES tb_runs(id) ON DELETE CASCADE,
    coverage_target  FLOAT          NOT NULL,
    fab_no           INTEGER,       /* building identifier */
    phase_no         INTEGER,       /* phase */
    model_no         INTEGER,       /* data model type */
    toolset_code     VARCHAR(128),  /* single toolset/E2E code */
    e2e_group_nos    VARCHAR(8000)  /* comma-list of numeric group IDs */
);

-- 3. SCENARIO-RUN PARAMETERS
CREATE TABLE tb_run_scenario_params (
    id             VARCHAR(45)   PRIMARY KEY
                     REFERENCES tb_runs(id) ON DELETE CASCADE,
    scenario_code  VARCHAR(128)  NOT NULL,
    scenario_file  VARCHAR(512)  NOT NULL
);

-- 4. PATH DEFINITIONS
CREATE TABLE tb_path_definitions (
    id                      INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id                  VARCHAR(45)   NOT NULL
                             REFERENCES tb_runs(id) ON DELETE CASCADE,
    scope                   VARCHAR(32)   NOT NULL,     /* 'CONNECTIVITY','FLOW' */
    s_node_id               BIGINT        NOT NULL
                             REFERENCES nw_nodes(id),
    e_node_id               BIGINT
                             REFERENCES nw_nodes(id),
    definition_hash         VARCHAR(64)   NOT NULL,     /* dedupe key */

    toolset_code            VARCHAR(128), /* single code */
    e2e_group_nos           VARCHAR(8000),/* comma-list */

    filter_category_nos     VARCHAR(128), /* comma-list ints */
    filter_utility_nos      VARCHAR(900), /* comma-list ints */
    filter_reference_codes  VARCHAR(128), /* comma-list */

    created_at              TIMESTAMP     NOT NULL DEFAULT now()
);

-- 5. ATTEMPTED PATHS
CREATE TABLE tb_attempt_paths (
    id             INTEGER AUTO_INCREMENT PRIMARY KEY,
    run_id         VARCHAR(45)    NOT NULL
                    REFERENCES tb_runs(id) ON DELETE CASCADE,
    path_def_id    INTEGER        NOT NULL
                    REFERENCES tb_path_definitions(id) ON DELETE CASCADE,
    request_hash   VARCHAR(64)    NOT NULL,
    status         VARCHAR(20)    NOT NULL,  /* 'FOUND','NOT_FOUND' */
    picked_at      TIMESTAMP      NOT NULL DEFAULT now(),
    notes          VARCHAR(512)
);

-- 6. PATH EXECUTIONS
CREATE TABLE tb_path_executions (
    id               INTEGER AUTO_INCREMENT PRIMARY KEY,
    attempt_id       INTEGER        NOT NULL
                     REFERENCES tb_attempt_paths(id) ON DELETE CASCADE,
    path_hash        VARCHAR(64),
    node_count       INTEGER        NOT NULL,
    link_count       INTEGER        NOT NULL,
    total_length_mm  NUMERIC(15,3)  NOT NULL,
    coverage         FLOAT          NOT NULL,
    cost             DOUBLE PRECISION NOT NULL,
    validation_passed BOOLEAN,
    executed_at      TIMESTAMP      NOT NULL DEFAULT now()
);

-- 7. EXECUTION DETAIL TABLES
CREATE TABLE tb_exec_data_codes (
    exec_id    INTEGER NOT NULL
                  REFERENCES tb_path_executions(id) ON DELETE CASCADE,
    data_code  INTEGER NOT NULL,
    PRIMARY KEY(exec_id, data_code)
);
CREATE TABLE tb_exec_utilities (
    exec_id     INTEGER NOT NULL
                   REFERENCES tb_path_executions(id) ON DELETE CASCADE,
    utility_no  INTEGER NOT NULL,
    PRIMARY KEY(exec_id, utility_no)
);
CREATE TABLE tb_exec_references (
    exec_id        INTEGER     NOT NULL
                      REFERENCES tb_path_executions(id) ON DELETE CASCADE,
    reference_code VARCHAR(16) NOT NULL,
    PRIMARY KEY(exec_id, reference_code)
);
