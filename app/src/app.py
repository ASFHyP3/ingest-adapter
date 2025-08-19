import json
import os
import traceback

import boto3
import hyp3_sdk

import gunw
import opera_rtc_s1_slc


def get_job_dict(hyp3_url: str, username: str, password: str, job_id: str) -> dict:
    hyp3 = hyp3_sdk.HyP3(hyp3_url, username, password)
    job = hyp3.get_job_by_id(job_id)
    return job.to_dict()


def process_message(message: dict, edl_credentials: dict) -> None:
    username, password = edl_credentials['username'], edl_credentials['password']
    job = get_job_dict(message['hyp3_url'], username, password, message['job_id'])

    match job['job_type']:
        case 'ARIA_S1_GUNW': # TODO add additional job types
            gunw.process_job(job)
        case 'OPERA_RTC_S1_SLC':
            opera_rtc_s1_slc.process_job(job)
        case _:
            raise ValueError(f'Job type {job["job_type"]} is not supported')


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
