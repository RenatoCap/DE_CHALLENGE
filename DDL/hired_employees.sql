CREATE TABLE [migration_tables].[hired_employees] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [name] VARCHAR(255),
    [hire_datetime] VARCHAR(100),
    [department_id] INT,
    FOREIGN KEY (department_id) REFERENCES [migration_tables].[deparments](id),
    [job_id] INT,
    FOREIGN KEY (job_id) REFERENCES [migration_tables].[jobs](id)
);