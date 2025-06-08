import os


from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify
from modules.utils.execute_query import execute_query
from modules.utils.db_connection import get_sql_server_connection


LOG_FOLDER = os.path.join(Path(__file__).parent, 'log')

app = Flask(__name__)


@app.route('/api/v1/batch-insert', methods=['POST'])
def batch_insert():
    data = request.json()
    table_name = data.get('table')
    rows = data.get('row')

    if not table_name or not rows or len(rows) > 100:
        return jsonify({
            'error': 'Invalid input data or row count exceeds 1000',
            'code': 400
        })
        

@app.route('/api/v1/employees-by-quarter', methods=['GET'])
def hello_world():
    year_param = request.args.get('year')
    year = int(year_param)

    if not year:
        return jsonify({
            'error': 'Invalid input data or row count exceeds 1000',
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
            'error': 'Invalid input data or row count exceeds 1000',
            'code': 400
        })
    else:
        try:
            db_conn = get_sql_server_connection()
            query_results = execute_query('hire_employees', year, db_conn)
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


if __name__ == '__main__':
    app.run(debug=True)