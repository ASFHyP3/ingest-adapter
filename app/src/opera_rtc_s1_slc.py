import json
import os
from datetime import UTC, datetime
from pathlib import Path

import boto3
import requests


s3 = boto3.client('s3')
sqs = boto3.client('sqs')  # TODO: botocore.exceptions.NoRegionError: You must specify a region


def _get_granule_ur_pattern(granule_ur: str) -> str:
    return f'{granule_ur[:49]}*{granule_ur[64:]}'


# TODO: factor out util function and use for both aria and opera?
def _exists_in_cmr(cmr_domain: str, granule_ur: str) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = (
        ('short_name', 'OPERA_L2_RTC-S1_V1'),
        ('granule_ur', _get_granule_ur_pattern(granule_ur)),
        ('options[granule_ur][pattern]', 'true'),
        ('page_size', 1),
    )
    response = requests.get(url, params=params)
    response.raise_for_status()
    if response.json()['items']:
        print(f'{granule_ur} already exists in CMR as {response.json()["items"][0]["umm"]["GranuleUR"]}')
        return True
    return False


def _get_products(bucket: str, job: dict) -> list[dict]:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=job['job_id'])

    product_names = {Path(obj['Key']).stem for obj in response['Contents'] if obj['Key'].endswith('.h5')}

    return [
        {
            'name': product_name,
            'files': [
                {
                    'name': Path(obj['Key']).name,
                    'type': _get_file_type(obj['Key']),
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


def _get_file_type(key: str) -> str:
    if key.endswith('.tif') or key.endswith('.h5'):
        return 'data'
    elif key.endswith('.png'):
        return 'browse'
    elif key.endswith('.iso.xml'):
        return 'metadata'
    else:
        raise ValueError(f'Could not determine file type for {key}')


def _get_message(product: dict) -> dict:
    return {
        'identifier': product['name'],
        'collection': 'OPERA_L2_RTC-S1_V1',
        'version': '1.6.1',
        'submissionTime': datetime.now(tz=UTC).isoformat().replace('+00:00', 'Z'),
        'product': product,
        'provider': 'HyP3',
        'trace': 'ASF-TOOLS',
    }


def _send_messages(queue_url: str, messages: list[dict]) -> None:
    for message in messages:
        sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def process_job(job: dict) -> None:
    products = _get_products(os.environ['HYP3_CONTENT_BUCKET'], job)
    messages = [
        _get_message(product) for product in products if _exists_in_cmr(os.environ['CMR_DOMAIN'], product['name'])
    ]
    _send_messages(os.environ['QUEUE_URL'], messages)
