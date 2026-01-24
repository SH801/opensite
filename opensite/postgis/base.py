import os
import logging
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from opensite.logging.base import LoggingBase

class PostGISBase:
    def __init__(self, log_level=logging.INFO):
        self.log = LoggingBase("Logger", log_level)

        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.database = os.getenv("POSTGRES_DB", "opensite")
        self.user = os.getenv("POSTGRES_USER", "opensite")
        self.password = os.getenv("POSTGRES_PASSWORD", "#######")
        
        # Initialize a connection pool for efficiency
        try:
            self.pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.log.debug(f"Connected to database: {self.database}")
        except Exception as e:
            self.log.error(f"Error connecting to Postgres: {e}")

    def execute_query(self, query, params=None):
        """Standard wrapper to execute a command and commit it."""
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
        finally:
            self.pool.putconn(conn)

    def fetch_all(self, query, params=None):
        """Standard wrapper to fetch results as a list of dictionaries."""
        conn = self.pool.getconn()
        try:
            # Specifying RealDictCursor makes fetchall() return [ {'col': val}, ... ]
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        finally:
            self.pool.putconn(conn)

    def get_table_names(self, schema='public'):
        """
        Returns a set of physical table names present in the specified schema.
        Uses the standard fetch_all tuple-return format.
        """
        sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s 
            AND table_type = 'BASE TABLE'
        """
        results = self.fetch_all(sql, (schema,))
        
        return {row['table_name'] for row in results}