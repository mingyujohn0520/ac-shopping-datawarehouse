import sys
import os
import json
import subprocess
import datetime as dt
from datetime import datetime
from ac_shopping_crm_config import AcShoppingCrmConfig, PipelineConfig, TableConfig
from utils.postgressql_connector import PostgresSqlConnector
from utils.secrets import get_secret
from utils.tasks import BaseTask

BASE_PATH = "{}/datalake_etl/load_ac_shopping_crm/config/".format(os.getcwd())


class LoadAcShoppingCrm(BaseTask):
    def __init__(self, config_path=None, table_name=None):
        self.config_path = config_path

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-c",
            "--config_path",
            type=str,
            help="YAML config file path.",
            required=False,
        )

        args, _ = parser.parse_known_args()

    def configure(self, args):
        self.config_path = self.config_path or args.config_path

    def extract(self, table_config):
        pass

    def transform(self):
        return 1

    def load(self):
        return 1

    def main(self):
        main_config = AcShoppingCrmConfig(BASE_PATH + "ac_shopping_crm.yml")
        pipe_config = main_config.get_pipeline_config()
        print(pipe_config.destination_schema)

        table_config_list = main_config.get_table_config()

        print(table_config_list)

        for table_config in table_config_list:
            print(table_config.table_name)
