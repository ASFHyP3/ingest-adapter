import datetime
import json
import os
from pathlib import Path

import boto3

import ingest
import util


s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def _granule_ur_pattern(granule_ur: str) -> str:
    return f'{granule_ur[:49]}*{granule_ur[64:]}'


def _get_products(bucket: str, job_id: str) -> list[ingest.IngestProduct]:
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


def _get_message(product: ingest.IngestProduct) -> ingest.IngestMessage:
    return {
        'identifier': product['name'],
        'collection': ingest.Collection.OPERA_RTC_S1_SLC,
        'version': '1.6.1',
        'submissionTime': util.get_submission_time(),
        'product': product,
        'provider': ingest.PROVIDER,
        'trace': ingest.TRACE,
    }


def _send_messages(queue_url: str, messages: list[ingest.IngestMessage]) -> None:
    for message in messages:
        print(f'Publishing {message["identifier"]} to {queue_url}')
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def process_job(job: dict) -> None:
    products = _get_products(os.environ['HYP3_CONTENT_BUCKET'], job['job_id'])
    messages = [
        _get_message(product)
        for product in products
        if not util.exists_in_cmr(
            os.environ['CMR_DOMAIN'], ingest.Collection.OPERA_RTC_S1_SLC, product['name'], _granule_ur_pattern
        )
    ]
    _send_messages(os.environ['OPERA_RTC_QUEUE_URL'], messages)
