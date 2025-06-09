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

        file = open(os.path.join(queries_folder, f"{query_name}.sql"))
        query = file.read().replace("{year}", f"{year}")

        # Execute the query.
        cursor = db_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall() 
        db_conn.commit()

    except pyodbc.Error as ex:
        sqlstate = ex.args[0]
        print(f"Database error executing query '{query_name}.sql' for year {year}. SQLSTATE: {sqlstate}, Error: {ex}")
        if db_conn:
            db_conn.rollback()
            print(f"Transaction for query '{query_name}.sql' has been rolled back.")
        raise ex
    except FileNotFoundError:
        raise f"Error: SQL file '{query_name}.sql' not found at '{file_path}'."
    except Exception as e:
        print(f"An unexpected error occurred while executing query '{query_name}.sql' for year {year}: {e}")
        if db_conn:
            db_conn.rollback()
            print(f"Transaction for query '{query_name}.sql' has been rolled back due to an unexpected error.")
        raise e
    finally:
        if cursor:
            cursor.close()
            print(f"Cursor for query '{query_name}.sql' closed.")
            
    return results
    