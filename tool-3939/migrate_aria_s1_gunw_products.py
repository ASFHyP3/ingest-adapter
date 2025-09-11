import json
import pathlib

import aws
import gunw
import ingest_message
import util


def get_product_file(bucket: str, key: str) -> ingest_message.IngestProductFile:
    checksum, size = aws.md5_and_size_for_s3_file(bucket, key)
    return {
        'name': key,
        'type': gunw._get_file_type(key),
        'uri': f's3://{bucket}/{key}',
        'size': size,
        'checksum': checksum,
        'checksumType': 'md5',
    }


def generate_ingest_message(product_key: pathlib.Path) -> ingest_message.IngestMessage:
    files: list[ingest_message.IngestProductFile] = [
        get_product_file(bucket, key)
        for bucket, key in [
            ('ingest-prod-aux', str(product_key.with_suffix('.json'))),
            ('grfn-content-prod', str(product_key)),
            ('grfn-public-prod', str(product_key.with_suffix('.png'))),
        ]
    ]

    product_name = product_key.stem
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


message = generate_ingest_message(
    pathlib.Path('S1-GUNW-A-R-004-tops-20171118_20161111-230701-00079W_00039N-PP-f7d8-v3_0_0.nc')
)
print(json.dumps(message))
