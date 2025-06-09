SELECT
    d.department,
    j.job,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.datetime) = 1 THEN h.id END) AS Q1,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.datetime) = 2 THEN h.id END) AS Q2,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.datetime) = 3 THEN h.id END) AS Q3,
    COUNT(DISTINCT CASE WHEN DATEPART(qq, h.datetime) = 4 THEN h.id END) AS Q4
FROM migration_tables.hired_employees h
LEFT JOIN migration_tables.departments d ON d.id = h.department_id
LEFT JOIN migration_tables.jobs j ON j.id = h.job_id
WHERE YEAR(h.datetime) = {year} AND h.datetime IS NOT NULL
GROUP BY d.department, j.job
ORDER BY d.department, j.job;