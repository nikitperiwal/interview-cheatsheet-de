--#### Problem Statement:
--Write a SQL query to find all consecutive sequences of at least three events where the number of attendees (`people`) was 100 or more.
--
--##### Additional Requirements:
--1. The `id` values represent the natural order of events, so only consider consecutive `id` values when determining sequences.
--2. If an event does not meet the `people >= 100` condition, it breaks the sequence.
--3. Only return events that are part of a qualifying sequence of at least three consecutive records.
--4. The result should include the `id`, `dates`, and `people` columns, sorted by `id` in ascending order.
--
--#### Example Input:
--
--| id | dates      | people |
--|----|------------|--------|
--| 1  | 2024-02-20 | 80     |
--| 2  | 2024-02-21 | 150    |
--| 3  | 2024-02-22 | 200    |
--| 4  | 2024-02-23 | 120    |
--| 5  | 2024-02-24 | 90     |
--| 6  | 2024-02-25 | 130    |
--| 7  | 2024-02-26 | 140    |
--| 8  | 2024-02-27 | 60     |
--
--#### Expected Output:
--
--| id | dates      | people |
--|----|------------|--------|
--| 2  | 2024-02-21 | 150    |
--| 3  | 2024-02-22 | 200    |
--| 4  | 2024-02-23 | 120    |
--
--In this case:
--- The sequence `(2,3,4)` qualifies since all have `people >= 100`.
--- Sequence `(6,7)` doesn't qualifies since chunk is less than 3
--- Events `1` and `5` are excluded because they don't meet the criteria.
--
--#### Constraints:
--- Assume `id` values are unique and sequentially increasing but may have gaps.
--- The `people` count is always a non-negative integer.
--- The table may have millions of rows, so optimize for performance.
--

WITH filtered AS (
    SELECT id, dates, people,
           id - ROW_NUMBER() OVER (ORDER BY id) AS grp
    FROM stadium
    WHERE people >= 100
)
SELECT id, dates, people
FROM filtered
WHERE grp IN (
    SELECT grp
    FROM filtered
    GROUP BY grp
    HAVING COUNT(*) >= 3
)
ORDER BY id;
