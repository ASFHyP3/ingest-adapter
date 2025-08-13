import json
from datetime import datetime, timezone
from pathlib import Path

import boto3


s3 = boto3.client('s3')
sqs = boto3.client('sqs')


QUEUE_URL = ''
BUCKET = ''


def get_products(bucket: str, job_id: str) -> list[dict]:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=job_id)

    product_names = {Path(obj['Key']).stem for obj in response['Contents'] if obj['Key'].endswith('.h5')}

    return [
        {
            'name': product_name,
            'files': [
                {
                    'name': Path(obj['Key']).name,
                    'type': get_file_type(obj['Key']),
                    'uri': f's3://{bucket}/{obj["Key"]}',
                    'size': obj['Size'],
                    'checksum': obj['ETag'].strip('"'),
                    'checksumType': 'md5',
                } for obj in response['Contents'] if product_name in obj['Key']
            ],
            'dataVersion': '1.0',
        } for product_name in product_names
    ]


def get_file_type(key: str) -> str:
    if key.endswith('.tif') or key.endswith('.h5'):
        return 'data'
    elif key.endswith('.png'):
        return 'browse'
    elif key.endswith('.iso.xml'):
        return 'metadata'
    else:
        raise ValueError(f'Could not determine file type for {key}')


def get_message(product: dict) -> dict:
    return {
        'identifier': product['name'],
        'collection': 'OPERA_L2_RTC-S1_V1',
        'version': '1.6.1',
        'submissionTime': datetime.now(tz=timezone.utc).isoformat().replace('+00:00', 'Z'),
        'product': product,
        'provider': 'HyP3',
        'trace': 'ASF-TOOLS',
    }


def send_messages(queue_url: str, messages: list[dict]) -> None:
    for message in messages:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def process_job(job_id: str) -> None:
    products = get_products(job_id)
    messages = [get_message(product) for product in products]
    send_messages(QUEUE_URL, messages)
