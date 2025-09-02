import hashlib

import boto3


ONE_MB = 1024 * 1024
S3_CLIENT = boto3.client('s3')


def md5_for_s3_file(bucket: str, key: str, chunk_size: int = 5 * ONE_MB) -> str:
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)

    md5_hash = hashlib.md5()

    for chunk in response['Body'].iter_chunks(chunk_size):
        md5_hash.update(chunk)

    return md5_hash.hexdigest()
