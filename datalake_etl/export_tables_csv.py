import boto3
import pandas as pd
import io

def unload_incremental_from_redshift():
    


def read_prefix_to_df(bucket, incremental_prefix):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    prefix_objs = bucket.objects.filter(Prefix=incremental_prefix)
    prefix_df = []
    for obj in prefix_objs:
        key = obj.key
        body = obj.get()["Body"].read()
        temp = pd.read_csv(
            io.BytesIO(body), encoding="utf8", header="infer", delimiter=","
        )
        prefix_df.append(temp)
    return pd.concat(prefix_df)


def export_incremental_to_csv(incremental_prefix, outputt_file_name):
    incremental_df = read_prefix_to_df(bucket, incremental_prefix)
    incremental_df.to_csv(
        outputt_file_name, header="true", index=False, encoding="utf-8"
    )


bucket = "dna-redshift-export-stage"

incremental_prefix_customer = "test_copy/customers/"
incremental_prefix_customer = "test_copy/customers/"
incremental_prefix_customer = "test_copy/customers/"
incremental_prefix_customer = "test_copy/customers/"
incremental_prefix_customer = "test_copy/customers/"
incremental_prefix_customer = "test_copy/customers/"


export_incremental_to_csv(incremental_prefix, outputt_file_name)
export_incremental_to_csv(incremental_prefix, outputt_file_name)
export_incremental_to_csv(incremental_prefix, outputt_file_name)
export_incremental_to_csv(incremental_prefix, outputt_file_name)
export_incremental_to_csv(incremental_prefix, outputt_file_name)
export_incremental_to_csv(incremental_prefix, outputt_file_name)

