import json

##from data_ingestion_config import DataIngestionConfig

from utils.tasks import BaseTask
from utils.s3_connector import S3Connector
from utils.postgressql_connector import PostgresSqlConnector


from datetime import datetime

# import pendulum


class DataIngestionTask(BaseTask):
    def __init__(
        self, task_name, config_path=None, table_name=None, config_group=None,
    ):
        self.config_path = config_path
        self.table_name = table_name
        self.config_group = config_group
        self.s3_con = S3Connector()
        BaseTask.__init__(self, task_name)

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-c",
            "--config_path",
            type=str,
            help="YAML config file path.",
            required=False,
        )

        task_config.add_argument(
            "-t",
            "--table_name",
            type=str,
            help="""
                specific table to process in YAML (don't set parameter to run all)
            """,
            required=False,
        )

        args, _ = parser.parse_known_args()
        self.s3_con.configure(args)

    def configure(self, args):
        self.config_path = self.config_path or args.config_path
        self.table_name = self.table_name or args.table_name

    def _get_connection(self, credentials):

        return PostgresSqlConnector(credentials)

    def create_table(self, source_schema, source_table):

        df = postgre_conn.get_column_metadata("customer", "ac_shopping")

        table_sql = "create table if not exists ac_shopping_crm.customer ("

        row_sql_list = []
        for row in df.itertuples(index=False):
            # apply source to Redshift data type mappings
            row_sql = self._map_to_redshift_column(
                column=row.column_name,
                data_type=row.data_type,
                col_type=row.column_type,
                char_max_length=row.character_maximum_length,
                num_precision=row.numeric_precision,
                num_scale=row.numeric_scale,
            )

            row_sql_list.append(row_sql)

        create_table_sql = table_sql + "\n" + ",\n".join(row_sql_list) + ")"
        redshift_conn.e

    def _map_to_redshift_column(
        self, column, data_type, col_type, char_max_length, num_precision, num_scale,
    ):
        # character data types
        if data_type in [
            "varchar",
            "char",
            "character varying",
            "character",
        ]:
            return f"{column} {data_type}({int(char_max_length)}) encode zstd"
        # supported data types that can use az64
        elif data_type in [
            "bigint",
            "timestamp without time zone",
            "integer",
            "smallint",
        ]:
            return f"{column} {data_type} encode az64"
        # other supported data types
        elif data_type in [
            "boolean",
            "double precision",
            "real",
            "timestamp with time zone",
            "date",
            "time",
            "timetz",
            "time without time zone",
            "time with time zone",
        ]:
            return f"{column} {data_type} encode zstd"
        # data types which require precision and scale
        elif data_type in ["numeric"]:
            return f"{column} {data_type}({int(num_precision)},{int(num_scale)}) encode az64"
        else:
            return f"{column} varchar(max) encode zstd"

    def _get_formatted_columns(self, columns, transforms={}):
        # quote columns in case reserved words are used
        quote = "`" if self.source_platform == "mysql" else '"'
        col_string = ""
        comma = ""
        print(f"columns: {columns}, transforms: {transforms}")
        for col in columns:
            # apply column transformations if supplied
            col_value = transforms[col] if col in transforms else f"{quote}{col}{quote}"
            col_string += f"{comma}{col_value}"
            comma = ", "

        return col_string

    def main(self):
        ##self.s3_con.list_bucket()

        postgre_conn = self._get_connection("postgres_ac_master")

        redshift_conn = self._get_connection("redshift_ac_master")

        df = postgre_conn.get_column_metadata("customer", "ac_shopping")

        table_sql = "create table if not exists ac_shopping_crm.customer ("

        row_sql_list = []
        for row in df.itertuples(index=False):
            # apply source to Redshift data type mappings
            row_sql = self._map_to_redshift_column(
                column=row.column_name,
                data_type=row.data_type,
                col_type=row.column_type,
                char_max_length=row.character_maximum_length,
                num_precision=row.numeric_precision,
                num_scale=row.numeric_scale,
            )

            row_sql_list.append(row_sql)

        print(table_sql + "\n" + ",\n".join(row_sql_list) + ")")

