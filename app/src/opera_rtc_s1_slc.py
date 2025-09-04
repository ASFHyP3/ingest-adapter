import json
import os
from pathlib import Path

import boto3

import ingest_message
import util


s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _granule_ur_pattern(granule_ur: str) -> str:
    return f'{granule_ur[:49]}*{granule_ur[64:]}'


def _get_products(bucket: str, job_id: str) -> list[ingest_message.IngestProduct]:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=job_id)

    product_names = {Path(obj['Key']).stem for obj in response['Contents'] if obj['Key'].endswith('.h5')}

    return [
        {
            'name': product_name,
            'files': [
                {
                    'name': Path(obj['Key']).name,
                    'type': util.get_file_type(obj['Key']),
                    'uri': f's3://{bucket}/{obj["Key"]}',
                    'size': obj['Size'],
                    'checksum': obj['ETag'].strip('"'),
                    'checksumType': 'md5',
                }
                for obj in response['Contents']
                if product_name in obj['Key']
            ],
            'dataVersion': '1.0',
        }
        for product_name in product_names
    ]


def _get_message(product: ingest_message.IngestProduct) -> ingest_message.IngestMessage:
    return {
        'identifier': product['name'],
        'collection': ingest_message.OPERA_RTC_COLLECTION,
        'version': ingest_message.CNM_SCHEMA_VERSION,
        'submissionTime': util.get_submission_time(),
        'product': product,
        'provider': ingest_message.PROVIDER,
        'trace': ingest_message.TRACE,
    }


def _send_messages(queue_url: str, messages: list[ingest_message.IngestMessage]) -> None:
    for message in messages:
        print(f'Publishing {message["identifier"]} to {queue_url}')
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def process_job(job: dict) -> None:
    products = _get_products(os.environ['HYP3_CONTENT_BUCKET'], job['job_id'])
    messages = [
        _get_message(product)
        for product in products
        if not util.exists_in_cmr(
            os.environ['CMR_DOMAIN'],
            ingest_message.OPERA_RTC_COLLECTION,
            product['name'],
            _granule_ur_pattern,
        )
    ]
    _send_messages(os.environ['OPERA_RTC_QUEUE_URL'], messages)
