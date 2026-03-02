
import snowflake.connector
import os

class SnowflakeConnector:
    """
    A generic Snowflake connector class that uses user ID and password for authentication.
    It retrieves credentials from environment variables.
    """

    def __init__(self):
        self.user = os.environ.get("SNOWFLAKE_USER")
        self.password = os.environ.get("SNOWFLAKE_PASSWORD")
        self.account = os.environ.get("SNOWFLAKE_ACCOUNT")
        self.warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
        self.database = os.environ.get("SNOWFLAKE_DATABASE")
        self.schema = os.environ.get("SNOWFLAKE_SCHEMA")

        if not all([self.user, self.password, self.account, self.warehouse, self.database, self.schema]):
            raise ValueError("Snowflake credentials (USER, PASSWORD, ACCOUNT, WAREHOUSE, DATABASE, SCHEMA) must be set as environment variables.")

    def get_connection(self):
        """
        Establishes and returns a Snowflake connection.
        """
        try:
            conn = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            return conn
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Snowflake: {e}")

    def execute_query(self, query, params=None):
        """
        Executes a query and returns the results.
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params)
            # Fetch column names
            columns = [col[0] for col in cursor.description]
            # Fetch all rows as a list of dictionaries
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            cursor.close()
            return results
        finally:
            if conn:
                conn.close()

    def execute_statement(self, statement, params=None):
        """
        Executes a DML/DDL statement.
        """
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(statement, params)
            conn.commit()
            cursor.close()
            return {"status": "success", "message": "Statement executed successfully."}
        finally:
            if conn:
                conn.close()
