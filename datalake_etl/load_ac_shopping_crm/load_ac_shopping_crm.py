import sys
import os
import json
import subprocess
import datetime as dt
from datetime import datetime


class LoadAcShoppingCrm:
    def __init__(self, config_file_path=None, table_name=None):
        self.config_file_path = config_file_path
        self.table_name = table_name

    def args(self, parser):
        task_config = parser.add_argument_group("Task Arguments")
        task_config.add_argument(
            "-c",
            "--config_file_path",
            type=str,
            help="YAML config file path.",
            required=False,
        )
        task_config.add_argument(
            "-a",
            "--table_name",
            type=str,
            help="""
                table name to process
            """,
            required=False,
        )

        args, _ = parser.parse_known_args()

    def configure(self, args):
        self.config_file_path = self.config_file_path or args.config_file_path
        self.table_name = self.table_name or args.table_name

    def extract():
        return 1

    def transform():
        return 1

    def load():
        return 1

    def main():
        extract()
        transform()
        load()
