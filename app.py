import os
import json
import pyodbc
import logging
import pandas as pd

from pathlib import Path
from fastavro import reader
from datetime import datetime
from modules.table_rules.jobs import Job
from flask import Flask, request, jsonify
from modules.utils.execute_query import execute_query
from modules.table_rules.department import Departments
from modules.table_rules.hired_employees import HiredEmployees
from modules.utils.db_connection import get_sql_server_connection
from modules.utils.avro_functions import get_avro_schema_for_table
from modules.utils.blob_storage_connection import get_blob_service_client


LOG_FOLDER = os.path.join(Path(__file__).parent, 'log', 'rejected_api')
TEMP_DIR = os.path.join(Path(__file__).parents[0], 'tmp')

rejected_rows_logger = logging.getLogger('rejected_rows_logger')


app = Flask(__name__)

@app.route('/api/v1/batch-insert', methods=['POST'])
def batch_insert():
    data = request.json
    table_name = data['table_name']
    rows = data['rows']

    if not isinstance(data, dict):
        return jsonify({
            'error': 'Invalid JSON format. Expected a dictionary.',
            'code': 400
        }), 400
    
    table_name: str = data.get('table_name')
    rows: list = data.get('rows')
    if not table_name:
        return jsonify({
            'error': 'Missing "table_name" in request payload.',
            'code': 400
        }), 400
    
    if not rows:
        return jsonify({
            'error': 'Missing "rows" data in request payload.',
            'code': 400
        }), 400

    if not isinstance(rows, list):
        return jsonify({
            'error': '"rows" field must be a list.',
            'code': 400
        }), 400
    
    BATCH_LIMIT = 1000
    if len(rows) > BATCH_LIMIT:
        return jsonify({
            'error': f'Row count exceeds the limit of {BATCH_LIMIT}.',
            'code': 400
        }), 400
    
    if table_name == 'jobs':
        validator = Job(rows)

    elif table_name == 'departments':
        validator = Departments(rows)

    elif table_name == 'hired_employees':
        validator = HiredEmployees(rows)

    else:
        return jsonify({
            'error': f'Table name "{table_name}" not recognized or supported by the system.',
            'code': 400
        })
    
    insert_columns = validator.insert_columns
    accepted, rejected = validator.validate_schema()

    if not rejected_rows_logger.handlers:
        file_handler = logging.FileHandler(os.path.join(LOG_FOLDER, f'rejected_{table_name}_{datetime.now().strftime("%Y_%m_%d_%I_%M_%S")}.log'), mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(message)s')
        file_handler.setFormatter(formatter)
        rejected_rows_logger.addHandler(file_handler)
        rejected_rows_logger.setLevel(logging.WARNING)

    if rejected:
        for rejected_entry in rejected:
            rejected_rows_logger.warning(json.dumps(rejected_entry, ensure_ascii=False))
    
    # --- Lógica de inserción en la base de datos ---
    try:
        db_conn = get_sql_server_connection()
        cursor = db_conn.cursor()
        
        placeholders = ', '.join(['?' for _ in insert_columns])
        insert_sql = f"INSERT INTO migration_tables.{table_name} ({', '.join(insert_columns)}) VALUES ({placeholders})"
        

        df_accepted = pd.DataFrame(accepted)
        if not df_accepted.empty:
            data_to_insert = [tuple(row) for row in df_accepted[insert_columns].values]
            cursor.executemany(insert_sql, data_to_insert)
            db_conn.commit()
            inserted_count = len(data_to_insert)
        else:
            db_conn.commit()
            inserted_count = 0

    except Exception as e:
        if db_conn:
            db_conn.rollback()
        app.logger.error(f"Database insertion failed for table {table_name}: {e}", exc_info=True)
        return jsonify({
            'error': 'An error occurred during database insertion.',
            'details': str(e),
            'code': 500
        }), 500
    finally:
        if cursor:
            cursor.close()
        if db_conn:
            db_conn.close()
    
    response_message = f"Batch insert processed. Accepted {inserted_count} rows, rejected {len(rejected)} rows."
    if rejected:
        response_message += " See logs/rejected_rows_YYYY_MM_DD.log for details on rejected rows."

    return jsonify({
        'status': 1,
        'message': response_message,
        'data': [{
            'inserted': f"{inserted_count} rows",
            'rejected': f"{len(rejected)} rows"
        }],
        'metadata': {
            'version': '1.0.0',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    }), 200


@app.route('/api/v1/employees-by-quarter', methods=['GET'])
def hello_world():
    year_param = request.args.get('year')
    year = int(year_param)

    if not year:
        return jsonify({
            'error': 'Parameter year is necessary',
            'code': 400
        })
    else:
        try:
            db_conn = get_sql_server_connection()
            query_results = execute_query('count_quartes', year, db_conn)
            formatted_results = []
            for row in query_results:
                formatted_results.append({
                    "department": row[0],
                    "job": row[1],
                    "Q1": row[2],
                    "Q2": row[3],
                    "Q3": row[4],
                    "Q4": row[5]
                })

            return jsonify({
                "status": 1,
                "message": "success",
                "data": formatted_results,
                "metadata": {
                    "version": "1.0.0",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }), 200

        except Exception as e:
            if db_conn:
                db_conn.rollback()
            raise


@app.route('/api/v1/employees-hired', methods=['GET'])
def employees_hired():
    year_param = request.args.get('year')
    year = int(year_param)

    if not year:
        return jsonify({
            'error': 'Parameter year is necessary',
            'code': 400
        })
    else:
        try:
            db_conn = get_sql_server_connection()
            query_results = execute_query('hired_employees', year, db_conn)
            formatted_results = []
            
            for row in query_results:
                formatted_results.append({
                    "id": row[0],
                    "department": row[1],
                    "hired": row[2]
                })

            return jsonify({
                "status": 1,
                "message": "success",
                "data": formatted_results,
                "metadata": {
                    "version": "1.0.0",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }), 200

        except Exception as e:
            if db_conn:
                db_conn.rollback()
            raise


@app.route('/api/v1/restored-table', methods=['POST'])
def restored_table():
    data = request.json
    if not data:
        return jsonify({
            'error': 'No JSON payload provided.',
            'code': 400
        })

    table_name = data['table_name']
    avro_blob_path = data['avro_file_path_in_blob']

    full_table_name_sql = f"migration_tables.{table_name}"
    avro_schema = get_avro_schema_for_table(table_name)

    os.makedirs(TEMP_DIR, exist_ok=True)

    local_avro_path = os.path.join(TEMP_DIR, os.path.basename(avro_blob_path))
    db_conn = None
    container_client = None
    temp_blob_service_client = get_blob_service_client()
    container_client = temp_blob_service_client.get_container_client("backup")
    blob_client = container_client.get_blob_client(avro_blob_path)
    with open(local_avro_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())

    records = []
    with open(local_avro_path, 'rb') as fo:
        avro_reader = reader(fo)
        for record in avro_reader:
            records.append(record)
        if not records:
            return jsonify({"status": "warning", "message": "No records found in AVRO file, nothing restored."}), 200

    db_conn = get_sql_server_connection()
    cursor = db_conn.cursor()
    avro_fields = [f['name'] for f in avro_schema['fields'] if f['name'] != 'id']
    columns_sql = ", ".join([f"{col}" for col in avro_fields])
    placeholders = ", ".join(["?"] * len(avro_fields))

    try:
        cursor.execute(f"TRUNCATE TABLE {full_table_name_sql}")
        db_conn.commit()
    except Exception as e:
        db_conn.rollback()

    insert_sql = f"INSERT INTO {full_table_name_sql} ({columns_sql}) VALUES ({placeholders})"
    inserted_count = 0

    for record in records:
        values = [record.get(field_name) for field_name in avro_fields]
        try:
            cursor.execute(insert_sql, tuple(values))
            inserted_count += 1
        except pyodbc.Error as e:
            db_conn.rollback()

    db_conn.commit()
    db_conn.close()
    os.remove(local_avro_path)

    return jsonify({
        'status': 1,
        'message': "Success",
        'data': [{
            'inserted': f"{inserted_count} rows",
        }],
        'metadata': {
            'version': '1.0.0',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    })
    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
