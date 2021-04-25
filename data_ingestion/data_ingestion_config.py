import yaml
import time
import os
from utils.postgressql_connector import PostgresSqlConnector


class DataIngestionConfig:
    """
    Class for dealing with data ingestion config. Reads the given YAML file and
    resolves it to a PipelineConfig class and TableConfig class
    :param yaml_file: The YAML filename and path (without .yml) to process in
        data_ingestion/config/
    :type yaml_file: str
    :param table_id: (Optional) The specific table name to process.
        If not set, all tables will be processed
    :type table_id: str
    :param config_group: (Optional) Currently not in use. If needed, we can use
        it to specify a config_group to process.
        If not set, all config_groups will be processed
    :type config_group: str
    """

    def __init__(
        self, yaml_file, table_id=None, config_group=None,
    ):
        self.yaml_file = yaml_file
        self.table_id = table_id
        self.config_group = config_group
        self.yaml_string = self._get_yaml()

    def _get_yaml(self):
        print(f"working dir: {os.getcwd()}")
        print(f"yaml file: {self.yaml_file}")
        with open(self.yaml_file) as config_file:
            return yaml.full_load(config_file)

    def get_yaml_attr(self, attribute_name):
        return self.yaml_string.get(attribute_name)

    def get_pipeline_config(self):
        return PipelineConfig(self)

    def get_table_config(self):
        pipe_config = self.get_pipeline_config()
        table_config_list = []
        for table in self.yaml_string.get("tables"):
            for table_name, table_config_attr in table.items():
                table_config_list.append(
                    TableConfig(table_name, pipe_config, table_config_attr)
                )
        return table_config_list


class PipelineConfig:
    def __init__(self, config):
        # resolve pipeline level config
        self.dag_name = config.get_yaml_attr("dag_name")
        self.s3_bucket_name = config.get_yaml_attr("s3_bucket_name")
        self.destination_platform = (
            config.get_yaml_attr("destination_platform") or "Redshift"
        )
        self.destination_schema = config.get_yaml_attr("destination_schema")
        self.destination_credentials = (
            config.get_yaml_attr("destination_credentials") or "redshift_ac_master"
        )
        self.source_platform = config.get_yaml_attr("source_platform") or "postgres"
        self.source_credentials = config.get_yaml_attr("source_credentials")
        self.source_schema = config.get_yaml_attr("source_schema")
        self.staging_schema = config.get_yaml_attr("staging_schema")
        self.copy_extra_params = (
            config.get_yaml_attr("copy_extra_params")
            or "CSV GZIP delimiter AS ',' NULL AS 'NULL' TRUNCATECOLUMNS IGNOREHEADER 1"
        )
        self.source_conn = self._get_connection(config)

    def _get_connection(self, config):

        return PostgresSqlConnector(self.source_credentials)


class TableConfig:
    def __init__(self, table, pipe_config, table_attr):
        self._resolve_parameters(table, pipe_config, table_attr)

    def _resolve_parameters(self, table, pipe_config, table_attr):
        # resolve table level config
        print(f"Resolve table level config")
        self.load_columns = None
        self.source_table = table
        self.update_method = table_attr.get("update_method")
        self.destination_table = table_attr.get("destination_table") or table
        # self.staging_schema = table_attr.get("staging_schema") or "staging"
        self.staging_table = (
            table_attr.get("staging_table")
            or f"{pipe_config.destination_schema}_{self.destination_table}"
        )
        self.filename = f"{table}.csv.gz"
        self.parameter_name = f"{pipe_config.dag_name}/{table}"

        self.s3_object = table_attr.get("s3_object")
        self.table_copy_params = table_attr.get("table_copy_params") or ""
        # always specify columns when executing a COPY under certain circumstances
        self.specify_copy_columns = (
            True
            # ifexplicitly specifying column inclusions
            if table_attr.get("column_inclusions")
            else False
        )
        # set DB specific attributes
        if pipe_config.source_platform != "s3":
            # generate column inclusions from source metadata if not provided in config
            self.column_inclusions = table_attr.get(
                "column_inclusions"
            ) or self._resolve_column_inclusions(pipe_config=pipe_config)
            print(f"column_inclusions:{self.column_inclusions}")
            # generate update keys from source PKs if not provided in config
            self.update_keys = table_attr.get(
                "update_keys"
            ) or self._resolve_update_keys(pipe_config=pipe_config)
            # convert load column array to a LoadColumnConfig collection
            # self.load_columns = self._resolve_load_columns(
            #     table_attr.get("load_columns")
            # )
            # self.has_parameters = True if len(self.load_columns) > 0 else False
            self.column_transformations = self._resolve_transformations(
                table_attr.get("column_transformations")
            )

        else:
            self.update_keys = table_attr.get("update_keys")
            self.column_inclusions = table_attr.get("column_inclusions")

        self.sort_key = self._resolve_sort_key(table_attr.get("sort_key"))

    def _resolve_sort_key(self, sort_key):
        # if sort key not provided use first load_column or update key
        if sort_key:
            return sort_key
        elif self.load_columns:
            return self.load_columns[0].load_column
        elif self.update_keys:
            return self.update_keys[0]
        else:
            return None

    def _resolve_transformations(self, transformations):
        if not transformations:
            return {}
        else:
            trans_dict = {}
            # update to return all transformations
            for column_trans in transformations:
                trans_dict.update(column_trans)

            return trans_dict

    def _resolve_column_inclusions(self, pipe_config):
        # get columns from source system
        df = pipe_config.source_conn.get_column_metadata(
            table_name=self.source_table, schema_name=pipe_config.source_schema
        )

        return df["column_name"].tolist()

    def _resolve_update_keys(self, pipe_config):
        # get primary key(s) from source system
        print(f"{pipe_config.source_platform}")
        print(f"{self.source_table}")
        print(f"{pipe_config.source_schema}")
        print(f"{pipe_config.source_conn}")
        return pipe_config.source_conn.get_table_primary_keys(
            table_name=self.source_table, schema_name=pipe_config.source_schema
        )

    # def _resolve_load_columns(self, load_columns_yml):
    #     load_col_list = []

    #     # create a list of LoadColumnConfig objects
    #     if load_columns_yml:
    #         print(f"load_columns_yml:{load_columns_yml}")
    #         for load_columns in load_columns_yml:
    #             print(f"load_columns:{load_columns}")
    #             for load_column, load_column_attr in load_columns.items():
    #                 print(
    #                     f"load_column:{load_column}, load_column_attr: {load_column_attr}"
    #                 )
    #                 load_expression = load_column_attr.get("load_expression")
    #                 param_sql = load_column_attr.get("param_sql")
    #                 load_col_list.append(
    #                     LoadColumnConfig(load_column, load_expression, param_sql)
    #                 )

    #     return load_col_list


class LoadColumnConfig:
    def __init__(self, load_column, load_expression, param_sql):
        self.load_column = load_column
        self.load_expression = load_expression
        self.param_sql = param_sql
