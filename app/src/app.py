import datetime
import json
import os
import pathlib
import traceback

import boto3
import hyp3_sdk
import requests


def get_granule_ur_pattern(granule_ur: str) -> str:
    return granule_ur.rsplit('-', 1)[0] + '-*'


def exists_in_cmr(cmr_domain: str, granule_ur: str) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = (
        ('short_name', 'ARIA_S1_GUNW'),
        ('granule_ur', get_granule_ur_pattern(granule_ur)),
        ('options[granule_ur][pattern]', 'true'),
        ('page_size', 1),
    )
    response = requests.get(url, params=params)
    response.raise_for_status()
    if response.json()['items']:
        print(f'{granule_ur} already exists in CMR as {response.json()["items"][0]["umm"]["GranuleUR"]}')
        return True
    return False


def generate_ingest_message(hyp3_job_dict: dict) -> dict:
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


def publish_message(message: dict, topic_arn: str) -> None:
    print(f'Publishing {message["ProductName"]} to {topic_arn}')
    topic_region = topic_arn.split(':')[3]
    sns = boto3.client('sns', region_name=topic_region)
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
    )


def get_job_dict(hyp3_url: str, username: str, password: str, job_id: str) -> dict:
    hyp3 = hyp3_sdk.HyP3(hyp3_url, username, password)
    job = hyp3.get_job_by_id(job_id)
    return job.to_dict()


def process_message(message: dict, edl_credentials: dict) -> None:
    username, password = edl_credentials['username'], edl_credentials['password']
    job = get_job_dict(message['hyp3_url'], username, password, message['job_id'])

    if job['job_type'] != 'ARIA_S1_GUNW':
        raise ValueError(f'Job type {job["job_type"]} is not supported; must be ARIA_S1_GUNW')
    ingest_message = generate_ingest_message(job)
    if not exists_in_cmr(os.environ['CMR_DOMAIN'], ingest_message['ProductName']):
        publish_message(ingest_message, os.environ['INGEST_TOPIC_ARN'])


def load_credentials() -> dict:
    secret_arn = os.environ['SECRET_ARN']
    secretsmanager = boto3.client('secretsmanager')

    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    credentials = json.loads(response['SecretString'])

    return credentials


def lambda_handler(event: dict, _) -> dict:
    batch_item_failures = []

    credentials = load_credentials()

    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['Message'])
            process_message(message, credentials)
        except Exception:
            print(traceback.format_exc())
            print(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}
