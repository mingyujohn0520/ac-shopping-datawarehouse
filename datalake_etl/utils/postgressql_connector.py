import psycopg2
import pandas as pd
from utils.secrets import get_secret


class PostgresSqlConnector:
    def __init__(self, secret_name=None):
        self.secret_name = secret_name
        self.credentials = get_secret(self.secret_name)

    def get_connection(self):
        try:
            connection = psycopg2.connect(
                host=self.credentials.get("host"),
                dbname=self.credentials.get("database"),
                port=self.credentials.get("port"),
                user=self.credentials.get("username"),
                password=self.credentials.get("password"),
            )
        except Exception as e:
            print(e)
            raise e
        return connection

    def get_execute_sql_result(self, sql, connection=None):
        if connection is None:
            connection = self.get_connection()

        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            result = cursor.fetchall()
        except Exception as e:
            raise e
        else:
            return result

    def export_sql_to_csv(self, sql, export_file_name, connection=None):
        if connection is None:
            connection = self.get_connection()

        export_df = pd.read_sql(sql, connection)
        export_df.to_csv(export_file_name, header="true", index=False)

