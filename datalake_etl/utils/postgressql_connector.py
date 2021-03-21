import traceback
import psycopg2
from psycopg2 import Error
from psycopg2 import sql
import pandas as pd
import csv


class PostgresSqlConnector:
    def __get_connection(self):
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

    def export_sql(
        self, query, filename, delimiter, postgres_connection=None, params=None,
    ):
        if not postgres_connection:
            connection = self.__get_connection()
        else:
            connection = postgres_connection
        try:
            cursor = connection.cursor()
            try:
                cursor.execute(query, params)

                with gzip.open(filename, "wt") as f:
                    writer = csv.writer(
                        f, quoting=csv.QUOTE_ALL, delimiter=delimiter, escapechar="\\"
                    )
                    writer.writerow(col[0] for col in cursor.description)
                    rows = cursor.fetchall()
                    # for row in rows:
                    #     writer.writerow("NULL" if x is None else x for x in row)
                    writer.writerows(rows)

            finally:
                cursor.close()

        except Exception as e:
            print(e)
            raise e
