import responses

import gunw
import util


@responses.activate
def test_exists_in_cmr():
    responses.get(
        'https://cmr.earthdata.nasa.gov/search/granules.umm_json',
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {
                    'short_name': 'ARIA_S1_GUNW',
                    'granule_ur': 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-*',
                    'options[granule_ur][pattern]': 'true',
                    'page_size': 1,
                },
            ),
        ],
        json={
            'items': [
                {
                    'umm': {
                        'GranuleUR': 'myGranule',
                    },
                },
            ],
        },
    )
    granule_ur = 'S1-GUNW-D-R-036-tops-20250131_20241226-041630-00025E_00035N-PP-99eb-v3_0_1'
    assert util.exists_in_cmr('cmr.earthdata.nasa.gov', 'ARIA_S1_GUNW', granule_ur, gunw._granule_ur_pattern)

    responses.get(
        'https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json',
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {
                    'short_name': 'ARIA_S1_GUNW',
                    'granule_ur': 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-*',
                    'options[granule_ur][pattern]': 'true',
                    'page_size': 1,
                },
            ),
        ],
        json={
            'items': [],
        },
    )
    granule_ur = 'S1-GUNW-D-R-123-tops-20230605_20230512-032645-00038E_00036N-PP-f518-v3_0_0'
    assert not util.exists_in_cmr('cmr.uat.earthdata.nasa.gov', 'ARIA_S1_GUNW', granule_ur, gunw._granule_ur_pattern)
