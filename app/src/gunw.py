import json
import os
import pathlib
from dataclasses import dataclass

import boto3

import util


sqs = boto3.client('sqs')


@dataclass(frozen=True)
class JobTypeIngestConfig:
    hyp3_urls: list[str]
    user_id: str


GUNW_USERNAME = 'access_cloud_based_insar'
A19_URL = 'https://hyp3-a19-jpl.asf.alaska.edu'
TIBET_URL = 'https://hyp3-tibet-jpl.asf.alaska.edu'


INGEST_CONFIGS = {
    'ARIA_S1_GUNW': None,
    'INSAR_ISCE': JobTypeIngestConfig(hyp3_urls=[A19_URL, TIBET_URL], user_id=GUNW_USERNAME),
    'ARIA_RAIDER': JobTypeIngestConfig(hyp3_urls=[A19_URL], user_id=GUNW_USERNAME),
}


def _granule_ur_pattern(granule_ur: str) -> str:
    return granule_ur.rsplit('-', 1)[0] + '-*'


def _generate_ingest_message(hyp3_job_dict: dict) -> dict:
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    product_key = pathlib.Path(hyp3_job_dict['files'][0]['s3']['key'])

    return {
        'ProductName': product_key.stem,
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


# TODO: consider moving to util (since it's copy-pasted from opera_rtc)
def _publish_message(message: dict, queue_url: str) -> None:
    print(f'Publishing {message["identifier"]} to {queue_url}')
    sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))


def _qualifies_for_ingest(job: dict, hyp3_url: str) -> bool:
    ingest_config = INGEST_CONFIGS[job['job_type']]

    if ingest_config is None:
        return True

    if hyp3_url not in ingest_config.hyp3_urls:
        print(f'Skipping ingest for {job} because HyP3 URL {hyp3_url} not in {ingest_config.hyp3_urls}')
        return False

    if (user_id := job['user_id']) != ingest_config.user_id:
        print(f'Skipping ingest for {job} because user {user_id} != {ingest_config.user_id}')
        return False

    return True


def process_job(job: dict, hyp3_url: str) -> None:
    if _qualifies_for_ingest(job, hyp3_url):
        ingest_message = _generate_ingest_message(job)
        if not util.exists_in_cmr(
            os.environ['CMR_DOMAIN'], 'ARIA_S1_GUNW', ingest_message['ProductName'], _granule_ur_pattern
        ):
            _publish_message(ingest_message, os.environ['GUNW_QUEUE_URL'])
