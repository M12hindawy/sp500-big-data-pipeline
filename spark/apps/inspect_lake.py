import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

print("--- MINIO LAKE INSPECTION ---")
buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
print("Buckets:", buckets)


for prefix in ["bronze/", "silver/", "gold/", "checkpoints/"]:
    res = s3.list_objects_v2(Bucket="stock-data", Prefix=prefix)
    items = [o["Key"] for o in res.get("Contents", [])]
    print(f"Prefix '{prefix}': {len(items)} objects")
    for k in items[:5]:
        print(f"  - {k}")

