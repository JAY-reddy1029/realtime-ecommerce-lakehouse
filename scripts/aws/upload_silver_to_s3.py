import boto3
import os

BUCKET_NAME = "jayreddy-ecommerce-lakehouse"
LOCAL_SILVER_PATH = "data/silver"
S3_PREFIX = "silver"

s3 = boto3.client("s3")

def upload_folder(local_path, bucket, s3_prefix):
    uploaded_count = 0
    for root, dirs, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_path)
            s3_key = f"{s3_prefix}/{relative_path}".replace("\\", "/")

            s3.upload_file(local_file_path, bucket, s3_key)
            uploaded_count += 1
            print(f"Uploaded: {s3_key}")

    return uploaded_count

print(f"Starting upload from '{LOCAL_SILVER_PATH}' to s3://{BUCKET_NAME}/{S3_PREFIX}/\n")

total = upload_folder(LOCAL_SILVER_PATH, BUCKET_NAME, S3_PREFIX)

print(f"\nUpload complete. {total} files uploaded to S3.")