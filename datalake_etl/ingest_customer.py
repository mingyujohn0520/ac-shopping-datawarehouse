import boto3

s3_client = boto3.client(
    "s3",
    aws_access_key_id="AKIA2P7EBZVPLAJPQ3MP",
    aws_secret_access_key="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",
    region_name="ap-southeast-2",
)


print(s3_client.list_buckets())
