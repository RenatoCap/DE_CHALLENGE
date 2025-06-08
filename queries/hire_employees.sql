WITH Hirings AS (
    SELECT
        b.id,
        b.name,
        YEAR(TRY_CAST(a.hire_datetime AS DATETIME)) AS year_hired,
        COUNT(DISTINCT a.id) AS hired
    FROM
        [migration_tables].[hired_employees] a
    LEFT JOIN
        [migration_tables].[deparments] b ON b.id = a.department_id
    WHERE
        TRY_CAST(a.hire_datetime AS DATETIME) IS NOT NULL
    GROUP BY
        b.id, b.name, YEAR(TRY_CAST(a.hire_datetime AS DATETIME))
)
SELECT
    id,
    name as department,
    SUM(hired) AS hired
FROM
    Hirings
GROUP BY
    id, name
HAVING
    SUM(hired) > (SELECT AVG(hired) FROM Hirings WHERE year_hired = 2021)
ORDER BY
    SUM(hired) DESC;