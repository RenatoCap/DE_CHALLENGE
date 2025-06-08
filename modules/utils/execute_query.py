import os
import pyodbc

from pathlib import Path

def execute_query(query_name, year, db_conn):
    """
    Executes a parameterized SQL query stored in an external .sql file,
    dynamically substituting a year placeholder before execution. Includes
    robust error handling with logging.

    :param query_name: The name of the SQL file to execute (without the .sql extension).
    :param year: The year value to be inserted into the SQL query.
    :param db_conn: An already established and open SQL Server database connection object.

    :returns: A list of tuples containing the fetched results if the query is a SELECT statement.
    """
    results = []
    cursor = None
    
    try:
        queries_folder = os.path.join(Path(__file__).parents[2], 'queries')
        file_path = open(os.path.join(queries_folder, f"{query_name}.sql"))

        with open(file_path, 'r') as file:
            query = file.read()

        query = query.replace("{year}", str(year))

        # Execute the query.
        cursor = db_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall() 
        db_conn.commit()
        cursor.close()

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database error executing query '{query_name}.sql' for year {year}. SQLSTATE: {sqlstate}, Error: {ex}", exc_info=True)
        if db_conn:
            db_conn.rollback()
            print(f"Transaction for query '{query_name}.sql' has been rolled back.")
    except FileNotFoundError:
        print(f"Error: SQL file '{query_name}.sql' not found at '{file_path}'.", exc_info=True)
    except Exception as e:
        print(f"An unexpected error occurred while executing query '{query_name}.sql' for year {year}: {e}", exc_info=True)
        if db_conn:
            db_conn.rollback()
            print(f"Transaction for query '{query_name}.sql' has been rolled back due to an unexpected error.")
    finally:
        if cursor:
            cursor.close()
            print(f"Cursor for query '{query_name}.sql' closed.")
    return results
    