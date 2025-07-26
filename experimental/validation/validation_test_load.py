results = self.db.query(sql)
if not results:
    return []

tests = []
last_id = None
test = None

for row in results:
    if not row:
        continue

    test_id = row[0] or None
    if not test_id:
        continue

    # When a new test_id is found and we already have a test built, append it
    if test and test_id != last_id:
        tests.append(test)
        test = None

    # If starting a new test instance
    if not test:
        scope = ValidationScope.nomalize(row[3])
        severity = Severity.nomalize(row[4])
        test_type = ValidationTestType.nomalize(row[5])

        test = ValidationTest(
            id=test_id,
            code=row[1],
            name=row[2],
            scope=scope,
            severity=severity,
            test_type=test_type,
            is_active=True,
            outcomes=[]
        )
        last_id = test_id

    # Always build the outcome and append to current test
    outcome = ValidationOutcome(
        validation_test_id=test_id,
        tag_type=row[6],
        tag_code=row[7],
        object_type=row[8],
        tag=row[9],
    )
    test.outcomes.append(outcome)

# Append the last test after loop
if test:
    tests.append(test)

return tests
