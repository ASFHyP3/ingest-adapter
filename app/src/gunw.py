import datetime
import json
import os
import pathlib

import boto3

import util


def _granule_ur_pattern(granule_ur: str) -> str:
    return granule_ur.rsplit('-', 1)[0] + '-*'


def _generate_ingest_message(hyp3_job_dict: dict) -> dict:
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    product_key = pathlib.Path(hyp3_job_dict['files'][0]['s3']['key'])

    return {
        'ProductName': product_key.stem,
        'DeliveryTime': datetime.datetime.now(tz=datetime.UTC).replace(tzinfo=None).isoformat(), # TODO delete
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
    if not util.exists_in_cmr(
        os.environ['CMR_DOMAIN'], 'ARIA_S1_GUNW', ingest_message['ProductName'], _granule_ur_pattern
    ):
        _publish_message(ingest_message, os.environ['INGEST_TOPIC_ARN'])
