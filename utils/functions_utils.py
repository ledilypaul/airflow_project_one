import psycopg2
from datetime import date
from utils.db_connection import db_connection

def insert_into_file_list(data):
    try:
        engine = db_connection()
        with engine.connect() as conn:
            query = """
                INSERT INTO file_list (file_name, file_path, last_modified, ingested_at)
                VALUES (%s, %s, %s,%s)
            """
            print(f"Inserting {data[0]} into the database")
            conn.execute(query, (data[0], data[1], data[2], date.today()))
        # conn.commit()
    except psycopg2.DatabaseError as e:
        raise psycopg2.DatabaseError(f"Error during insertion attempt = {e}")

def list_file_from_db():
    try:
        engine = db_connection()
        with engine.connect() as conn:
            query = """
                SELECT * FROM file_list 
                WHERE ingested_at = CURRENT_DATE;
            """
            result = conn.execute(query) 
            return result.fetchall()
    except psycopg2.DatabaseError as e:
        raise psycopg2.DatabaseError(f"Error during insertion attempt = {e}")
            