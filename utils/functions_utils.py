from db_connection import db_connection

def insert_into_file_list(data):
    conn = db_connection
    try:
        with conn.cursor() as cursor:
            query = """
                INSERT INTO file_list (file_name, file_path, last_modified)
                VALUES %s
                ON CONFLICT (file_path) DO NOTHING
            """
            execute_values(cursor, query, data)
            conn.commit()
    finally:
        conn.close()