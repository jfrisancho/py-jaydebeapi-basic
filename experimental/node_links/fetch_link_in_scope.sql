-- Note: This syntax is for PostgreSQL. It will differ for other databases.
-- The handling of an array/list of e2e_group_nos is vendor-specific.
CREATE OR REPLACE FUNCTION featch_links_in_scope(
    p_fab_no INT,
    p_model_no INT,
    p_phase_no INT,
    p_e2e_group_nos INT[] -- Use an array type for the list of groups
)
RETURNS TABLE(id BIGINT) AS $$
BEGIN
    -- This query is the same as the one we built in Python
    RETURN QUERY
    WITH filtered_nodes AS (
        SELECT n.id
        FROM nw_node n
        -- Conditionally join org_shape only if p_phase_no is provided
        LEFT JOIN org_shape sh ON sh.node_id = n.id AND p_phase_no IS NOT NULL
        WHERE
            (p_fab_no IS NULL OR n.fab_no = p_fab_no)
            AND (p_model_no IS NULL OR n.model_no = p_model_no)
            AND (p_phase_no IS NULL OR sh.phase_no = p_phase_no)
            -- Check if the array is empty or if the node's group is in the array
            AND (cardinality(p_e2e_group_nos) = 0 OR n.e2e_group_no = ANY(p_e2e_group_nos))
    )
    SELECT l.id FROM nw_link l
    WHERE l.s_node_id IN (SELECT id FROM filtered_nodes)

    UNION

    SELECT l.id FROM nw_link l
    WHERE l.e_node_id IN (SELECT id FROM filtered_nodes)

    ORDER BY id;
END;
$$ LANGUAGE plpgsql;


