from utils.db_connection import db_connection

def insert_into_file_list(data):
    try:
        conn = db_connection()
        with conn.cursor() as cursor:
            query = """
                INSERT INTO file_list (file_name, file_path, last_modified)
                VALUES (%s, %s, %s)
            """
            print(f"Inserting {data[0]} into the database")
            cursor.execute(query, (data[0], data[1], data[2]))
        conn.commit()
    except Exception as e:
        raise Exception(f"Error during insertion attempt = {e}")
