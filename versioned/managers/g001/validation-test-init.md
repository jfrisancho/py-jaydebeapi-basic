```sql
-- Insert the utility consistency validation test
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, is_active, description
) VALUES (
    'UTILITY_CONSISTENCY',
    'Utility Consistency Validation',
    'CONNECTIVITY',
    'MEDIUM',
    'LOGICAL',
    1,
    'Checks that all nodes in a path have non-null utility values and, for simple cases, that all utilities are consistent along the path.'
);

-- Insert utility validation outcomes
INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'PASS', 'All nodes have consistent and non-null utility values'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';

INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'MISSING_UTILITY', 'One or more nodes have missing utility values'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';

INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'INCONSISTENT_UTILITY', 'Utility value changes unexpectedly along the path'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';



-- Insert the utility consistency validation test
INSERT INTO tb_validation_tests (
    code, name, scope, severity, test_type, is_active, description
) VALUES (
    'UTILITY_CONSISTENCY',
    'Utility Consistency Validation',
    'CONNECTIVITY',
    'MEDIUM',
    'LOGICAL',
    1,
    'Checks that all nodes in a path have non-null utility values and, for simple cases, that all utilities are consistent along the path.'
);

-- Insert utility validation outcomes
INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'PASS', 'All nodes have consistent and non-null utility values'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';

INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'MISSING_UTILITY', 'One or more nodes have missing utility values'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';

INSERT INTO tb_validation_outcomes (
    validation_test_id, tag_type, tag_code, tag
)
SELECT id, 'QA', 'INCONSISTENT_UTILITY', 'Utility value changes unexpectedly along the path'
FROM tb_validation_tests WHERE code = 'UTILITY_CONSISTENCY';
