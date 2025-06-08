WITH Hirings AS (
    SELECT
        b.id,
        b.department,
        YEAR(TRY_CAST(a.datetime AS DATETIME)) AS year_hired,
        COUNT(DISTINCT a.id) AS hired
    FROM
        migration_tables.hired_employees a
    LEFT JOIN
        migration_tables.departments b ON b.id = a.department_id
    WHERE
        TRY_CAST(a.datetime AS DATETIME) IS NOT NULL
    GROUP BY
        b.id, b.department, YEAR(TRY_CAST(a.datetime AS DATETIME))
)
SELECT
    id,
	department,
    SUM(hired) AS hired
FROM
    Hirings
GROUP BY
    id, department
HAVING
    SUM(hired) > (SELECT AVG(hired) FROM Hirings WHERE year_hired = {year})
ORDER BY
    SUM(hired) DESC;