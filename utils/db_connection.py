import psycopg2
from psycopg2.extras import RealDictCursor

def db_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="apo_database",
            user="postgres",
            password="postgres"
        )
        return conn
    except Exception as e:
        raise psycopg2.Error(f"Error during connection attempt = {e}")
    
def close_connection(conn):
    if conn:
        conn.close()