from collections.abc import Callable

import ingest
import requests


def exists_in_cmr(
    cmr_domain: str, short_name: ingest.Collection, granule_ur: str, granule_ur_pattern: Callable[[str], str]
) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = {
        'short_name': short_name,
        'granule_ur': granule_ur_pattern(granule_ur),
        'options[granule_ur][pattern]': 'true',
        'page_size': '1',
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    if response.json()['items']:
        print(f'{granule_ur} already exists in CMR as {response.json()["items"][0]["umm"]["GranuleUR"]}')
        return True
    return False


def get_file_type(key: str) -> str:
    if key.endswith('.tif') or key.endswith('.h5'):
        return 'data'
    elif key.endswith('.png'):
        return 'browse'
    elif key.endswith('.iso.xml') or key.endswith('.json'):
        return 'metadata'
    else:
        raise ValueError(f'Could not determine file type for {key}')
