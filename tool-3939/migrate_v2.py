import json

import ingest_message
import util


def get_file_type_and_bucket(key: str) -> tuple[str, str]:
    if key.endswith('.nc'):
        return 'data', 'grfn-content-prod'
    elif key.endswith('.png'):
        return 'browse', 'grfn-public-prod'
    elif key.endswith('.json'):
        return 'metadata', 'ingest-prod-aux'
    else:
        raise ValueError(f'Could not determine file type for {key}')


def get_product_file(filename: str, file_info: dict) -> ingest_message.IngestProductFile:
    checksum, size = file_info['checksum'], file_info['size']
    file_type, bucket = get_file_type_and_bucket(filename)
    return {
        'name': filename,
        'type': file_type,
        'uri': f's3://{bucket}/{filename}',
        'size': size,
        'checksum': checksum,
        'checksumType': 'md5',
    }


def generate_ingest_message(product_name: str, product_files: dict) -> ingest_message.IngestMessage:
    files: list[ingest_message.IngestProductFile] = [
        get_product_file(filename, product_files[filename])
        for filename in [product_name + '.json', product_name + '.nc', product_name + '.png']
    ]

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


# TODO: double-check a sample message before running script for all products
def main() -> None:
    with open('products.json') as f:
        products = json.load(f)

    for product_name, product_files in products.items():
        message = generate_ingest_message(product_name, product_files)
        print(json.dumps(message, indent=2))
        # TODO: send message


if __name__ == '__main__':
    main()
