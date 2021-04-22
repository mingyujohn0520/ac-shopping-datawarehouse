import json
from .data_ingestion_config import DataIngestionConfig

from ..tasks import BaseTask
from ..utils.parameters import (
    get_parameter,
    set_parameter,
    parameter_exists,
)
from ..connectors.s3_connector import S3Connector
from ..connectors.postgressql_connector import PostgresSqlConnector
from ..connectors.mysql_connector import MySqlConnector
from ..connectors.mssql_connector import MsSqlConnector
from ..utils.sql_builder import (
    SelectCommandBuilder,
    CopyCommandBuilder,
    DeleteCommandBuilder,
    AppendCommandBuilder,
)

from datetime import datetime
import pendulum


class DataIngestionTask(BaseTask):
    def __init__(
        self, task_name, config_path=None, table_name=None, config_group=None,
    ):
        self.config_path = config_path
        self.table_name = table_name
        self.config_group = config_group
        self.s3_con = S3Connector(self)
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

    def _get_connection(self, platform, credentials):
        if platform == "mysql":
            return MySqlConnector(self, credentials, self.environment)
        if platform == "mssql":
            return MsSqlConnector(self, credentials, self.environment)
        else:
            return PostgresSqlConnector(self, credentials, self.environment)

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

    def _extract(self, table_config, source_schema, destination_schema):
        where_list = []
        where_sql = ""
        # create parameter store parameter if not exists
        if table_config.has_parameters:
            self._maintain_table_parameters(
                table_config=table_config,
                destination_schema=destination_schema,
                new_only=True,
            )

            for lc in table_config.load_columns:
                param_name = f"{table_config.parameter_name}_{lc.load_column}"

                param_value = get_parameter(param_name)

                # build where clause and inject parameters
                where_list.append(
                    lc.load_expression.format(
                        load_column=lc.load_column, param_value=param_value
                    )
                )

            where_sql = "WHERE "
            where_sql += " AND ".join(where_list)

        # TODO: To improve performance for large extracts, may need to introduce
        # batching / exporting to multiple files (e.g. every 100K rows)

        # export source data to CSV
        self.source_conn.export_sql(
            query=self._build_select_sql(
                self._get_formatted_columns(
                    table_config.column_inclusions, table_config.column_transformations,
                ),
                source_schema,
                table_config.source_table,
                where_sql,
            ),
            filename=table_config.filename,
            delimiter=",",
        )

    def _build_select_sql(self, columns, schema, table, where):
        select_cmd = SelectCommandBuilder()
        sql_statement = select_cmd.set_params(
            columns_=columns,
            target_schema_=schema,
            target_table_=table,
            where_clause_=where,
        ).build()
        return str(sql_statement)

    def _build_copy_sql(
        self,
        schema,
        table,
        s3_path,
        extra_copy_params,
        table_copy_params,
        copy_columns,
    ):
        copy_cmd = CopyCommandBuilder()
        sql_statement = copy_cmd.set_params(
            target_schema_=schema,
            target_table_=table,
            file_name_=s3_path,
            columns_=copy_columns,
            extra_copy_params_=f"{extra_copy_params} {table_copy_params}",
        ).build()
        return str(sql_statement)

    def _build_delete_sql(
        self,
        source_schema,
        source_table,
        destination_schema,
        destination_table,
        columns,
    ):
        delete_cmd = DeleteCommandBuilder()
        sql_statement = delete_cmd.set_params(
            source_schema_=source_schema,
            source_table_=source_table,
            target_schema_=destination_schema,
            target_table_=destination_table,
            columns_=columns,
        ).build()
        return str(sql_statement)

    def _build_append_sql(
        self, source_schema, source_table, destination_schema, destination_table
    ):
        append_cmd = AppendCommandBuilder()
        sql_statement = append_cmd.set_params(
            source_schema_=source_schema,
            source_table_=source_table,
            target_schema_=destination_schema,
            target_table_=destination_table,
        ).build()
        return str(sql_statement)

    def _ingest(self, table_config, pipe_config):
        destination_schema = pipe_config.destination_schema
        bucket_name = pipe_config.s3_bucket_name
        destination_table = table_config.destination_table
        staging_schema = table_config.staging_schema
        staging_table = table_config.staging_table
        include_load_dts = table_config.include_load_dts

        s3_path = f"s3://{bucket_name}/{table_config.s3_object}"

        # when including load_dts or explicitly specifying column inclusions
        # we need to specify cols in the COPY so we don't get col mismatch
        copy_columns = (
            table_config.column_inclusions if table_config.specify_copy_columns else []
        )

        copy_cmd = self._build_copy_sql(
            schema=staging_schema,
            table=staging_table,
            s3_path=s3_path,
            extra_copy_params=pipe_config.copy_extra_params,
            table_copy_params=table_config.table_copy_params,
            copy_columns=copy_columns,
        )

        # truncate staging
        self.destination_conn.execute_sql(
            f"truncate table {staging_schema}.{staging_table}"
        )

        # copy to staging
        self.destination_conn.execute_sql(query=copy_cmd)

        # delete/truncate from destination based on update method
        if table_config.update_method == "merge":
            # delete
            delete_sql = self._build_delete_sql(
                source_schema=staging_schema,
                source_table=staging_table,
                destination_schema=destination_schema,
                destination_table=destination_table,
                columns=table_config.update_keys,
            )
            self.destination_conn.execute_sql(query=delete_sql)
        elif table_config.update_method == "full_load":
            # truncate destination
            self.destination_conn.execute_sql(
                f"truncate table {destination_schema}.{destination_table}"
            )

        if include_load_dts:
            # update load_dts timestamp
            self.destination_conn.execute_sql(
                f"update {staging_schema}.{staging_table} set load_dts = getdate();"
            )

        # check if deduping is required
        if table_config.dedupe_logic:
            # insert to destination with dedupe logic
            self._insert_with_dedupe(table_config=table_config, pipe_config=pipe_config)
        else:
            # append from staging to destination
            append_sql = self._build_append_sql(
                source_schema=staging_schema,
                source_table=staging_table,
                destination_schema=destination_schema,
                destination_table=destination_table,
            )
            self.destination_conn.execute_sql(query=append_sql)

    def add_load_dts(self, table, schema):
        # get destination columns
        df = self.destination_conn.get_column_metadata(
            table_name=table, schema_name=schema,
        )
        column_list = df["column_name"].tolist()
        if "load_dts" not in column_list:
            # add load_dts to destination table
            self.destination_conn.execute_sql(
                query=f"""
                alter table {schema}.{table} 
                add load_dts timestamp encode zstd;
                """
            )

    def _create_tables(
        self, table_config, source_schema, destination_schema, source_platform
    ):
        # TODO: Add functionality to identify and handle schema changes
        # check if destination (and staging) tables exist
        staging_schema = table_config.staging_schema
        staging_table = table_config.staging_table
        destination_table = table_config.destination_table
        include_load_dts = table_config.include_load_dts

        dest_exists = self.destination_conn.table_exists(
            table_name=destination_table, schema_name=destination_schema
        )
        stage_exists = self.destination_conn.table_exists(
            table_name=staging_table, schema_name=staging_schema
        )

        if dest_exists and include_load_dts:
            # add load_dts to dest table if not already there
            self.add_load_dts(destination_table, destination_schema)

        if stage_exists and include_load_dts:
            # add load_dts to stage table if not already there
            self.add_load_dts(staging_table, staging_schema)

        # both tables already exists so can exit
        if dest_exists and stage_exists:
            return

        # create stage table like dest table if dest table already exists
        if not stage_exists and dest_exists:
            sql = f"""
            create table {staging_schema}.{staging_table}
            (like {destination_schema}.{destination_table});
            """

            self.destination_conn.execute_sql(query=sql)

        # TODO: Auto create table from S3 file
        # create dest table if not already there
        if not dest_exists and source_platform != "s3":
            # create schema if not already there
            self._create_schema(destination_schema)

            table_name = destination_table

            # get table metadata from source system
            df = self.source_conn.get_column_metadata(
                table_name=table_name,
                schema_name=source_schema,
                column_list=table_config.column_inclusions,
            )

            # build up Redshift create table statement
            table_sql = "create table if not exists {0}.{1} ("
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

                # check if we should set sort key on given column
                sort_sql = (
                    " SORTKEY" if table_config.sort_key == row.column_name else ""
                )
                table_sql += f"\n\t{row_sql}{sort_sql},"

            # add load_dts column if required
            load_dts_sql = (
                f",\nload_dts timestamp encode zstd\n" if include_load_dts else ""
            )

            table_sql = f"{table_sql[:-1]}\n{load_dts_sql})\n DISTSTYLE EVEN;"

            # create destination table in Redshift
            self.destination_conn.execute_sql(
                query=table_sql.format(destination_schema, table_name)
            )

            # create stage table
            if not stage_exists:
                self.destination_conn.execute_sql(
                    query=table_sql.format(staging_schema, staging_table)
                )

    def _map_to_redshift_column(
        self, column, data_type, col_type, char_max_length, num_precision, num_scale,
    ):
        if self.source_platform == "mysql":
            if data_type in ["varchar", "char", "decimal"]:
                return f"{column} {col_type} encode zstd"
            elif data_type in ["bigint", "date", "int", "smallint"]:
                return f"{column} {data_type} encode az64"
            elif data_type in ["datetime"]:
                return f"{column} timestamp encode az64"
            elif data_type in ["double"]:
                return f"{column} float8 encode zstd"
            elif data_type in ["mediumint"]:
                return f"{column} int  encode az64"
            elif data_type in ["tinyint"]:
                return f"{column} smallint encode az64"
            else:
                return f"{column} varchar(max) encode zstd"
        elif self.source_platform == "postgres":
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
        elif self.source_platform == "mssql":
            if data_type in ["char", "nchar", "varchar", "nvarchar"]:
                return f"{column} {data_type}({int(char_max_length)}) encode zstd"
            elif data_type in [
                "int",
                "integer",
                "bigint",
                "smallint",
                "date",
            ]:
                return f"{column} {data_type} encode az64"
            elif data_type in [
                "double precision",
                "float",
                "real",
                "date",
                "time",
            ]:
                return f"{column} {data_type} encode zstd"
            elif data_type in ["tinyint"]:
                return f"{column} smallint encode az64"
            elif data_type in [
                # "timestamp",
                "datetime",
                "datatime2",
                "smalldatetime",
            ]:
                return f"{column} timestamp encode az64"
            elif data_type in ["timestamp"]:
                return f"{column} varchar(100) encode zstd"
            elif data_type in ["datetimeoffset"]:
                return f"{column} timestamptz encode zstd"
            elif data_type in ["numeric", "decimal"]:
                return f"{column} {data_type}({int(num_precision)},{int(num_scale)}) encode az64"
            elif data_type in ["money"]:
                return f"{column} decimal (15,4) encode az64"
            elif data_type in ["smallmoney"]:
                return f"{column} decimal (6,4) encode az64"
            else:
                return f"{column} varchar(max) encode zstd"

    def _create_schema(self, schema_name):
        sql = f"create schema if not exists {schema_name};"
        self.destination_conn.execute_sql(query=sql)

    def _upload(self, filename, bucket_name, object_name):
        # Copy to Redshift-ready bucket
        self.s3_con.upload_file(
            file_name=filename, bucket=bucket_name, object_name=object_name
        )

    def process_s3_ingestion(self, table, pipe):
        # generate tables if not already there (staging if dest exists only)
        self._create_tables(
            table_config=table,
            source_schema=pipe.source_schema,
            destination_schema=pipe.destination_schema,
            source_platform=pipe.source_platform,
        )

        # move s3 files to processing folder
        src_object_prefix = table.s3_object
        bucket = pipe.s3_bucket_name
        dest_path = src_object_prefix.rpartition("/")[0]
        if (
            self.s3_con.move_by_prefix(
                bucket, src_object_prefix, bucket, f"{dest_path}/processing"
            )
            == 1
        ):
            print(
                f"No files found in S3 bucket {bucket} for the {src_object_prefix} prefix. Skipping ingestion."
            )
            return

        file_prefix = src_object_prefix.rpartition("/")[2]
        table.s3_object = f"{dest_path}/processing/{file_prefix}"

        # ingest from S3 to Redshift
        self._ingest(table_config=table, pipe_config=pipe)

        # Archive files in S3
        self._archive(
            bucket_name=bucket, object_name=table.s3_object,
        )

    def process_database_ingestion(self, table, pipe):
        # generate tables if not already there
        self._create_tables(
            table_config=table,
            source_schema=pipe.source_schema,
            destination_schema=pipe.destination_schema,
            source_platform=pipe.source_platform,
        )
        # export data from source table to CSV
        self._extract(
            table_config=table,
            source_schema=pipe.source_schema,
            destination_schema=pipe.destination_schema,
        )
        # upload CSV to S3
        self._upload(
            filename=table.filename,
            bucket_name=pipe.s3_bucket_name,
            object_name=table.s3_object,
        )
        # ingest from S3 to Redshift
        self._ingest(table_config=table, pipe_config=pipe)
        # TODO: plugin data validation framework to self._validate()
        # update param ready for next run
        if table.has_parameters:
            self._maintain_table_parameters(table, pipe.destination_schema)

        # Archive files in S3
        self._archive(
            bucket_name=pipe.s3_bucket_name, object_name=table.s3_object,
        )

    def main(self):
        # init config class
        di = DataIngestionConfig(
            yaml_file=f"{self.config_path}.yml",
            table_id=self.table_name,
            config_group=self.config_group,
            environment=self.environment,
        )

        pipe = di.get_pipeline_config()

        self.source_platform = pipe.source_platform
        self.source_conn = pipe.source_conn
        self.destination_conn = self._get_connection(
            pipe.destination_platform, pipe.destination_credentials
        )

        # get list of tables to process
        tables = di.get_table_config(pipe)

        try:
            for table in tables:
                if self.source_platform.lower() == "s3":
                    self.process_s3_ingestion(table, pipe)
                else:
                    self.process_database_ingestion(table, pipe)

        except Exception as e:
            raise e
