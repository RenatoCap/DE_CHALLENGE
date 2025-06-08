python historical_loader.py \
    --file_name=jobs.csv \
    --target_table_name=migration_tables.jobs \
    --target_columns='["id", "job"]' \
    --insert_columns='["job"]'


python historical_loader.py \
    --file_name=hired_employees.csv \
    --target_table_name=migration_tables.hired_employees \
    --target_columns='["id", "name", "datetime", "department_id", "job_id"]' \
    --insert_columns='["name", "datetime", "department_id", "job_id"]'

python historical_loader.py \
    --file_name=departments.csv \
    --target_table_name=migration_tables.departments \
    --target_columns='["id", "department"]' \
    --insert_columns='["department"]'