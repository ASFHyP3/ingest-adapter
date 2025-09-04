import hashlib
import json

import boto3

import ingest_message


ONE_MB = 1024 * 1024 * 1024
S3_CLIENT = boto3.client('s3')
SQS_CLIENT = boto3.client('sqs')


def md5_for_s3_file(bucket: str, key: str, chunk_size: int = 5 * ONE_MB) -> str:
    response = S3_CLIENT.get_object(Bucket=bucket, Key=key)

    md5_hash = hashlib.md5()

    for chunk in response['Body'].iter_chunks(chunk_size):
        md5_hash.update(chunk)

    return md5_hash.hexdigest()


def list_objects_for_job(bucket: str, job_id: str) -> dict:
    return S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=job_id)


def send_ingest_message(queue_url: str, message: ingest_message.IngestMessage) -> None:
    print(f'Publishing {message["identifier"]} to {queue_url}')
    SQS_CLIENT.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
