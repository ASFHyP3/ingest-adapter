import json
import os
import pathlib
from dataclasses import dataclass

import boto3

import aws
import ingest_message
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


def _generate_ingest_message(hyp3_job_dict: dict) -> ingest_message.IngestMessage:
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    response = aws.S3_CLIENT.list_objects_v2(Bucket=bucket, Prefix=hyp3_job_dict['job_id'])

    files: list[ingest_message.IngestProductFile] = [
        {
            'name': pathlib.Path(obj['Key']).name,
            'type': util.get_file_type(obj['Key']),
            'uri': f's3://{bucket}/{obj["Key"]}',
            'size': obj['Size'],
            'checksum': aws.md5_for_s3_file(bucket, obj['Key']),
            'checksumType': 'md5',
        }
        for obj in response['Contents']
    ]

    product_name = pathlib.Path(hyp3_job_dict['files'][0]['s3']['key']).stem
    product: ingest_message.IngestProduct = {
        'name': product_name,
        'files': files,
        'dataVersion': '1.0',
    }

    return {
        'identifier': product_name,
        'collection': ingest_message.CmrCollection.ARIA_S1_GUNW.value,
        'version': '1.6.1',
        'submissionTime': util.get_submission_time(),
        'product': product,
        'provider': ingest_message.PROVIDER,
        'trace': ingest_message.TRACE,
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
        message = _generate_ingest_message(job)
        if not util.exists_in_cmr(
            os.environ['CMR_DOMAIN'],
            ingest_message.CmrCollection.ARIA_S1_GUNW.value,
            message['identifier'],
            _granule_ur_pattern,
        ):
            _publish_message(message, os.environ['GUNW_QUEUE_URL'])
