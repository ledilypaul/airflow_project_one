import psycopg2
from datetime import date
from utils.db_connection import db_connection
def insert_into_file_list(data):
    try:
        conn = db_connection()
        with conn.cursor() as cursor:
            query = """
                INSERT INTO file_list (file_name, file_path, last_modified, ingested_at)
                VALUES (%s, %s, %s,%s)
            """
            print(f"Inserting {data[0]} into the database")
            cursor.execute(query, (data[0], data[1], data[2], date.today()))
        conn.commit()
    except psycopg2.DatabaseError as e:
        raise psycopg2.DatabaseError(f"Error during insertion attempt = {e}")

def list_file_from_db():
    try:
        conn = db_connection()
        with conn.cursor() as cursor:
            query = """
                SELECT * FROM file_list 
                WHERE ingested_at = CURRENT_DATE;
            """
            cursor.execute(query)
            return cursor.fetchall()
    except psycopg2.DatabaseError as e:
        raise psycopg2.DatabaseError(f"Error during insertion attempt = {e}")
            