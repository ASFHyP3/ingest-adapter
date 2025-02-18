import datetime
import json
import os
import pathlib

import boto3
import hyp3_sdk
import requests



def get_granule_ur_pattern(granule_ur: str) -> str:
    return granule_ur.rsplit('-', 1)[0] + '-*'


def exists_in_cmr(cmr_domain: str, granule_ur: str) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = {
        'short_name': 'ARIA_S1_GUNW',
        'granule_ur': get_granule_ur_pattern(granule_ur),
        'options[granule_ur][pattern]': 'true',
        'page_size': 1,
    }
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
        'DeliveryTime': datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None).isoformat(),
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


def publish_message(message: dict, topic_arn: str):
    print(f'Publishing {message['ProductName']} to {topic_arn}')
    topic_region = topic_arn.split(':')[3]
    sns = boto3.client(topic_arn, region=topic_region)
    sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message),
    )


def process_message(message: dict):
    hyp3 = hyp3_sdk.HyP3(url=message['hyp3_url'], username=os.environ['EDL_USERNAME'], password=os.environ['EDL_PASSWORD'])
    job = hyp3.get_job_by_id(message['job_id'])
    ingest_message = generate_ingest_message(job.to_dict())
    if exists_in_cmr(ingest_message['ProductName'], os.environ['CMR_DOMAIN']):
        publish_message(message, os.environ['TOPIC_ARC'])


def lambda_handler(event: dict, context: object) -> dict:
    batch_item_failures = []
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['Message'])
            process_message(message)
        except Exception as e:
            print(f'Could not process message {record["messageId"]}')
            print(e)
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}
