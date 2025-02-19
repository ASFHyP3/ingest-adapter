import argparse
import json
import os

import boto3


def publish(hyp3_api_url: str, job_id: str, topic_arn: str) -> None:
    message = {
        'hyp3_api_url': hyp3_api_url,
        'job_id': job_id,
    }
    region = topic_arn.split(':')[3]
    sns = boto3.client('sns', region_name=region)
    sns.publish(TopicArn=topic_arn, Message=json.dumps(message))


def get_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('job_id', type=str)
    parser.add_argument('--hyp3-url', type=str, default=os.getenv('HYP3_URL'))
    parser.add_argument('--topic-arn', type=str, default=os.getenv('TOPIC_ARN'))
    parsed_args = parser.parse_args(args)

    if parsed_args.hyp3_url is None:
        raise ValueError('HyP3 URL must be provided via the --hyp3-url option or the HYP3_URL environment variable')

    if parsed_args.topic_arn is None:
        raise ValueError('Topic ARN must be provided via the --topic-arn option or the TOPIC_ARN environment variable')

    return parsed_args


def main() -> None:
    args = get_args()
    publish(args.hyp3_url, args.job_id, args.topic_arn)


if __name__ == '__main__':
    main()
