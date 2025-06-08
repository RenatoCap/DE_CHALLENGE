SELECT
    d.name,
    j.job,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.hire_datetime) = 1 THEN h.id END) AS Q1,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.hire_datetime) = 2 THEN h.id END) AS Q2,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.hire_datetime) = 3 THEN h.id END) AS Q3,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.hire_datetime) = 4 THEN h.id END) AS Q4
FROM [migration_tables].[hired_employees] h
LEFT JOIN [migration_tables].[deparments] d ON d.id = h.department_id
LEFT JOIN [migration_tables].[jobs] j ON j.id = h.job_id
WHERE YEAR(h.hire_datetime) = 2021 AND h.hire_datetime IS NOT NULL
GROUP BY d.name, j.job
ORDER BY d.name, j.job;