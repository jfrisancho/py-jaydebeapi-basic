CREATE TABLE nw_nodes (
    id BIGINT PRIMARY KEY NOT NULL,
    guid VARCHAR(64) NOT NULL,
    
    fab_no TINYINT NOT NULL,
    model_no TINYINT NOT NULL,
    data_code INTEGER NOT NULL,
    utility_no INTEGER,
    e2e_group_no INTEGER NOT NULL,
    e2e_header_id INTEGER NOT NULL,
    item_no INTEGER NOT NULL,
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