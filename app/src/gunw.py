import os
import pathlib
from dataclasses import dataclass

import aws
import ingest_message
import util


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


def _get_file_type(key: str) -> str:
    if key.endswith('.nc'):
        return 'data'
    elif key.endswith('.png'):
        return 'browse'
    elif key.endswith('.json'):
        return 'metadata'
    else:
        raise ValueError(f'Could not determine file type for {key}')


def _generate_ingest_message(hyp3_job_dict: dict) -> ingest_message.IngestMessage:
    bucket = hyp3_job_dict['files'][0]['s3']['bucket']
    response = aws.list_objects_for_job(bucket, hyp3_job_dict['job_id'])

    files: list[ingest_message.IngestProductFile] = [
        {
            'name': pathlib.Path(obj['Key']).name,
            'type': _get_file_type(obj['Key']),
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
        'dataVersion': '1',
    }

    return {
        'identifier': product_name,
        'collection': ingest_message.ARIA_S1_GUNW_COLLECTION,
        'version': ingest_message.CNM_SCHEMA_VERSION,
        'submissionTime': util.get_submission_time(),
        'product': product,
        'provider': ingest_message.PROVIDER,
        'trace': ingest_message.TRACE,
    }


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
            ingest_message.ARIA_S1_GUNW_COLLECTION,
            message['identifier'],
            _granule_ur_pattern,
        ):
            aws.send_ingest_message(os.environ['GUNW_QUEUE_URL'], message)
