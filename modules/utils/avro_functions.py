def get_avro_schema_for_table(table_name):
    """
    Returns the predefined AVRO schema for a given table name.
    You'll need to expand this for all your tables.
    """
    schemas = {
        "departments": {
            "type": "record",
            "name": "Department",
            "namespace": "migration_tables",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "department", "type": "string"}
            ]
        },
        "hired_employees": {
            "type": "record",
            "name": "HiredEmployee",
            "namespace": "migration_tables",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": ["null", "string"]},
                {"name": "datetime", "type": ["null", "string"]},
                {"name": "department_id", "type": ["null", "int"]},
                {"name": "job_id", "type": ["null", "int"]}
            ]
        },
        "jobs": {
            "type": "record",
            "name": "Job",
            "namespace": "migration_tables",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "job", "type": "string"}
            ]
        }
    }
    return schemas.get(table_name)