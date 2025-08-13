import datetime
import json
import os
import pathlib

import boto3
import requests


def _get_granule_ur_pattern(granule_ur: str) -> str:
    return granule_ur.rsplit('-', 1)[0] + '-*'


def _exists_in_cmr(cmr_domain: str, granule_ur: str) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = (
        ('short_name', 'ARIA_S1_GUNW'),
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


def _generate_ingest_message(hyp3_job_dict: dict) -> dict:
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    product_key = pathlib.Path(hyp3_job_dict['files'][0]['s3']['key'])

    return {
        'ProductName': product_key.stem,
        'DeliveryTime': datetime.datetime.now(tz=datetime.UTC).replace(tzinfo=None).isoformat(),
        'Browse': {
            'Bucket': bucket,
            'Key': str(product_key.with_suffix('.png')),
        },
        'Metadata': {
            'Bucket': bucket,
            'Key': str(product_key.with_suffix('.json')),
        },
        'Product': {
            'Bucket': bucket,
            'Key': str(product_key),
        },
    }


def _publish_message(message: dict, topic_arn: str) -> None:
    print(f'Publishing {message["ProductName"]} to {topic_arn}')
    topic_region = topic_arn.split(':')[3]
    sns = boto3.client('sns', region_name=topic_region)
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
    )


def process_job(job: dict) -> None:
    ingest_message = _generate_ingest_message(job)
    if not _exists_in_cmr(os.environ['CMR_DOMAIN'], ingest_message['ProductName']):
        _publish_message(ingest_message, os.environ['INGEST_TOPIC_ARN'])
