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
        except Error as e:
            error_detail = "{0}\n{1}".format(e, traceback.format_exc())
            self.parent_task.logging.log_error(
                "PostgresSql_connection_failed - Error while connecting to PostgresSql"
            )
            raise DBConnectionError("PostgresSql_connection", error_detail)
        return connection

    def export_sql(
        self,
        query,
        filename,
        delimiter,
        postgres_connection=None,
        params=None,
        **kwargs,
    ):
        if not postgres_connection:
            connection = self.__get_connection()
        else:
            connection = postgres_connection
        try:
            self.parent_task.logging.log_info("sql_execute: {}".format(query))
            self.parent_task.logging.log_info("param: {}".format(params))
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
            error_detail = "{0}\n{1}".format(e, traceback.format_exc())
            self.parent_task.logging.log_error(
                f"mysql_export_sql_failed: {error_detail}"
            )
            raise e

    def query_sql(
        self,
        query,
        postgres_connection=None,
        params=None,
        sql_identifiers=None,
        **kwargs,
    ):
        if not postgres_connection:
            connection = self.__get_connection()
        else:
            connection = postgres_connection
        try:
            if sql_identifiers is not None:
                ident = ast.literal_eval(sql_identifiers)
                sql_ident = dict()
                for (key, pair) in ident.items():
                    sql_ident[key] = sql.Identifier(pair)
                query = sql.SQL(query).format(**sql_ident)

            self.parent_task.logging.log_info("sql_execute: {}".format(query))
            return pd.read_sql(sql=query, con=connection, params=params, **kwargs)
        except Exception as e:
            error_detail = "{0}\n{1}".format(e, traceback.format_exc())
            self.parent_task.logging.log_error(
                f"PostgresSql_query_sql_failed: {error_detail}"
            )
            raise e

    def execute_sql(
        self, query, postgres_connection=None, params=None, sql_identifiers=None
    ):
        if not postgres_connection:
            connection = self.__get_connection()
        else:
            connection = postgres_connection
        try:
            connection.set_session(autocommit=True)
            self.parent_task.logging.log_info("sql_execute: {}".format(query))
            cursor = connection.cursor()
            try:
                if sql_identifiers is not None:
                    ident = ast.literal_eval(sql_identifiers)
                    sql_ident = dict()
                    for (key, pair) in ident.items():
                        sql_ident[key] = sql.Identifier(pair)
                    query = sql.SQL(query).format(**sql_ident)

                cursor.execute(query, params)
                connection.commit()
            finally:
                if connection:
                    cursor.close()
                    connection.close()

        except Exception as e:
            error_detail = "{0}\n{1}".format(e, traceback.format_exc())
            self.parent_task.logging.log_error(
                f"PostgresSql_execute_sql_failed: {error_detail}"
            )
            raise e

    def table_exists(self, table_name, schema_name):
        sql = (
            "select table_name from information_schema.tables "
            f"where table_name = '{table_name}' "
            f"and table_schema = '{schema_name}';"
        )

        df = self.query_sql(query=sql)

        if df.empty:
            return False
        else:
            return True

    def get_column_metadata(self, table_name, schema_name, column_list=[]):
        column_filter = ""
        if column_list:
            col_string = "'{0}'".format("', '".join(column_list))
            column_filter = f"and column_name in ({col_string.strip()}) "

        sql = f"""
            select
                c.column_name,
                c.data_type,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                null as column_type
            from information_schema.tables t
                left join information_schema.columns c
                    on t.table_schema = c.table_schema
                    and t.table_name = c.table_name
            where t.table_name = '{table_name}'
            and t.table_schema = '{schema_name}'
            {column_filter}
            order by c.ordinal_position;
        """

        df = self.query_sql(query=sql)

        return df

    def get_table_primary_keys(self, table_name, schema_name):
        # this is the only method that works in postgres and redshift
        sql = f"""
            SELECT attname column_name
            FROM pg_index ind, pg_class cl, pg_attribute att
            WHERE cl.oid = '{schema_name}.{table_name}'::regclass
            AND ind.indrelid = cl.oid
            AND att.attrelid = cl.oid
            AND att.attnum::text =
                ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' '))
            AND attnum > 0
            AND ind.indisprimary
            ORDER BY att.attnum;
        """

        df = self.query_sql(query=sql)
        return df["column_name"].tolist()
