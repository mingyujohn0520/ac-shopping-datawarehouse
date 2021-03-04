import boto3

# print(s3_client.list_buckets())
import psycopg2
import pandas

host = "ac-shopping-crm.cmxlethtljrk.ap-southeast-2.rds.amazonaws.com"
database = "ac_shopping_crm"
user = "ac_master"
password = "Datasquad2021"
port = 5432

conn = psycopg2.connect(
    host=host, database=database, user=user, password=password, port=port
)


# data frame

export_sql = "select * from ac_shopping.customer"

export_df = pandas.read_sql_query(export_sql, conn)

print(export_df.head(10))

export_df.to_csv("customer_test.csv", header="true")

s3_client = boto3.client(
    "s3",
    aws_access_key_id="AKIA2P7EBZVPLAJPQ3MP",
    aws_secret_access_key="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",
    region_name="ap-southeast-2",
)

bucket_name = "ac-shopping-datalake"

s3_client.upload_file(
    "customer_test.csv", bucket_name, "ac_shopping_crm/customer_test.csv"
)


redshift_host = (
    "ac-shopping-datawarehouse.cdzgizud15s1.ap-southeast-2.redshift.amazonaws.com"
)

redshift_database = "data_lake"
redshift_user = "ac_master"
redshift_password = "Datasquad2021"
redshift_port = 5439

redshift_conn = psycopg2.connect(
    host=redshift_host,
    database=redshift_database,
    user=redshift_user,
    password=redshift_password,
    port=redshift_port,
)

copy_table_query = """copy ac_shopping_crm.customer
    from 's3://ac-shoping-datalake/ac_shopping_crm/customer_test.csv'
    credentials 'aws_iam_role=arn:aws:iam::721495903582:role/aws-service-role/redshift.amazonaws.com/AWSServiceRoleForRedshift'
    CSV DELIMITER AS ','
    IGNOREHEADER 1"""

cursor = redshift_conn.cursor()
cursor.execute(copy_table_query)
