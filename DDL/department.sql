CREATE SCHEMA [migration_tables];

CREATE TABLE [migration_tables].[departments] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [department] NVARCHAR(255) NOT NULL
);