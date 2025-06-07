CREATE SCHEMA [migration_tables];

CREATE TABLE [migration_tables].[deparments] (
    [id] INT IDENTITY(1,1) PRIMARY KEY,
    [name] NVARCHAR(255) NOT NULL
);