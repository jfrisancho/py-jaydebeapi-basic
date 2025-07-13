CREATE TABLE tb_paths (
    id INTEGER AUTO_INCREMENT PRIMARY KEY,
    algorithm VARCHAR(64) NOT NULL,
    
    s_node_id BIGINT NOT NULL,
    e_node_id BIGINT,

    cost DOUBLE,
    created_at TIMESTAMP DEFAULT now() NOT NULL
);

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
    