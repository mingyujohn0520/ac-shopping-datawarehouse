import boto3
import psycopg2
import pandas
import json
from datetime import datetime
import datetime as dt


secrets_manager_client = boto3.client(
    "secretsmanager",
    region_name="ap-southeast-2",
    aws_access_key_id="AKIA2P7EBZVPLAJPQ3MP",
    aws_secret_access_key="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",
)


def get_secret(secret_name):

    client = boto3.client(
        "secretsmanager",
        region_name="ap-southeast-2",
        aws_access_key_id="AKIA2P7EBZVPHEO6M6TN",
        aws_secret_access_key="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",
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        return json.loads(secret)
    except Exception as e:
        print("Failed to retrive secret from secretsmanager " + str(e))
        raise ValueError


test = get_secret("postgres_ac_master")
print(test)


postgresql+psycopg2://airflow:airflow@localhost:5432/airflow

aws s3 cp s3://ac-shopping-airflow/airflow-webserver.service  /etc/systemd/system/airflow-webserver.service
aws s3 cp s3://ac-shopping-airflow/airflow-scheduler.service  /etc/systemd/system/airflow-scheduler.service



aws s3 cp s3://ac-shopping-airflow/airflow.cfg /home/airflow/airflow/airflow.cfg 
aws s3 cp s3://ac-shopping-airflow/dags/hello_word.py /home/airflow/airflow/dags/hello_word.py

aws s3 cp s3://ac-shopping-airflow/dags/sample_etl_job.py /home/airflow/airflow/dags/sample_etl_job.py


sudo chmod a+rwx /etc/systemd/system

sudo chown -R airflow:airflow /etc/systemd/system


 ssh -i "ac-shopping-airflow.pem" ubuntu@3.24.215.151