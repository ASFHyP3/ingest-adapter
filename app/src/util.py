from collections.abc import Callable

import requests


def exists_in_cmr(cmr_domain: str, short_name: str, granule_ur: str, granule_ur_pattern: Callable[[str], str]) -> bool:
    url = f'https://{cmr_domain}/search/granules.umm_json'
    params = (
        ('short_name', short_name),
        ('granule_ur', granule_ur_pattern(granule_ur)),
        ('options[granule_ur][pattern]', 'true'),
        ('page_size', 1),
    )
    response = requests.get(url, params=params)
    response.raise_for_status()
    if response.json()['items']:
        print(f'{granule_ur} already exists in CMR as {response.json()["items"][0]["umm"]["GranuleUR"]}')
        return True
    return False
