-- Problem 3: Transaction Analysis

-- Provided Table Structure
-- | uuid | user_type | txn_id | txn_amt | payment_option | bank | event_type | status_id | status   | ts           |
-- |------|-----------|--------|---------|----------------|------|------------|-----------|----------|--------------|
-- | u1   | solo      | t1     | 100     | UPI            | null | init       | s1        | null     | 145678945672 |
-- | u1   | solo      | null   | 100     | UPI            | null | return     | s1        | bank_fail| 145678945672 |
-- | u2   | solo      | t2     | 100     | CC             | HDFC | init       | s2        | null     | 145678945672 |
-- | u2   | solo      | null   | 100     | CC             | HDFC | return     | s2        | success  | 145678945672 |
-- | u2   | business  | t3     | 10000   | CC             | HDFC | init       | s3        | null     | 145678945672 |
-- | u2   | business  | null   | 10000   | CC             | HDFC | return     | s3        | success  | 145678945672 |

-- Queries Needed
-- 1. Attempts (fail, success, null) by user
-- 2. Attempts (fail, success, null) by user_type
-- 3. Attempts (fail, success, null) by user_type and bank
-- 4. Attempts (fail, success, null) by user and payment_option

-- 1. Attempts by User
SELECT
    uuid AS user,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS attempt_success,
    SUM(CASE WHEN status = 'bank_fail' THEN 1 ELSE 0 END) AS attempt_failure,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS attempt_null
FROM transactions
GROUP BY user;

-- 2. Attempts by User Type
SELECT
    user_type,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS attempt_success,
    SUM(CASE WHEN status = 'bank_fail' THEN 1 ELSE 0 END) AS attempt_failure,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS attempt_null
FROM transactions
GROUP BY user_type;

-- 3. Attempts by User Type and Bank
SELECT
    user_type,
    bank,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS attempt_success,
    SUM(CASE WHEN status = 'bank_fail' THEN 1 ELSE 0 END) AS attempt_failure,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS attempt_null
FROM transactions
GROUP BY user_type, bank;

-- 4. Attempts by User and Payment Option
SELECT
    uuid AS user,
    pmnt_option,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS attempt_success,
    SUM(CASE WHEN status = 'bank_fail' THEN 1 ELSE 0 END) AS attempt_failure,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS attempt_null
FROM transactions
GROUP BY user, pmnt_option;

-- Expected Output Structure
-- | user | user_type | bank | attempt_success | attempt_failure |
-- |------|-----------|------|-----------------|-----------------|
-- | u1   | null      | null | 100             | 20              |
-- | u1   | solo      | null | 100             | 20              |
-- | u1   | solo      | HDFC | 100             | 20              |
-- | null | solo      | HDFC | 100             | 20              |
-- | null | null      | HDFC | 100             | 20              |
-- |------|-----------|------|-----------------|-----------------|

SELECT
    COALESCE(uuid, 'null') AS user,
    COALESCE(user_type, 'null') AS user_type,
    COALESCE(bank, 'null') AS bank,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS attempt_success,
    SUM(CASE WHEN status = 'bank_fail' THEN 1 ELSE 0 END) AS attempt_failure
FROM transactions
GROUP BY
    ROLLUP(uuid, user_type, bank)
ORDER BY
    user, user_type, bank;
