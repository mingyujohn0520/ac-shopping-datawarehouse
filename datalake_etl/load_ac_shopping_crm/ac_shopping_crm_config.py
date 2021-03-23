import yaml
import time
import datetime
from datetime import datetime as dt
import os
import sys

sys.path.append(os.getcwd() + "/datalake_etl")
from utils.postgressql_connector import PostgresSqlConnector


class AcShoppingCrmConfig:
    """
    Class for dealing with load ac shopping crm ingestion config. Reads the given YAML file and
    resolves it to a PipelineConfig class and TableConfig class
    :param yaml_file: The YAML filename and path (without .yml) to process in
        ac_shopping_crm/config/
    :type yaml_file: str
    """

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.yaml_string = self._get_yaml()

    def _get_yaml(self):
        with open(self.yaml_file) as config_file:
            return yaml.full_load(config_file)

    def get_yaml_attr(self, attribute_name):
        return self.yaml_string.get(attribute_name)

    def get_pipeline_config(self):
        return PipelineConfig(self)

    def get_table_config(self, pipe_config):
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
        self.source_credentials = config.get_yaml_attr("source_credentials")
        self.source_schema = config.get_yaml_attr("source_schema")
        self.destination_schema = config.get_yaml_attr("destination_schema")
        self.destination_credentials = config.get_yaml_attr("destination_credentials")

    def _get_source_connection(self):
        return PostgresSqlConnector(self, self.source_credentials)

    def _get_destination_connection(self):
        return PostgresSqlConnector(self, self.destination_credentials)


class TableConfig:
    def __init__(
        self, table_name, pipe_config, table_config_attr,
    ):
        self._resolve_parameters(table_name, pipe_config, table_config_attr)

    def _resolve_parameters(self, table_name, pipe_config, table_config_attr):
        # resolve table ort level config
        self.table_name = table_name
        self.export_file_name = table_config_attr.get("export_file_name")
        self.load_columns = table_config_attr.get("load_columns")

        self.incremental_load_columns = table_config_attr.get(
            "incremental_load_columns"
        )
        self.copy_extra_params = table_config_attr.get("copy_extra_params")
