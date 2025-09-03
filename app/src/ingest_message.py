from typing import TypedDict


ARIA_S1_GUNW_COLLECTION = 'ARIA_S1_GUNW'
OPERA_RTC_COLLECTION = 'OPERA_L2_RTC-S1_V1'

PROVIDER = 'ASF_HyP3'
TRACE = 'ASF-TOOLS'


class IngestProductFile(TypedDict):
    name: str
    type: str
    uri: str
    size: int
    checksum: str
    checksumType: str


class IngestProduct(TypedDict):
    name: str
    files: list[IngestProductFile]
    dataVersion: str


class IngestMessage(TypedDict):
    identifier: str
    collection: str
    version: str
    submissionTime: str
    product: IngestProduct
    provider: str
    trace: str
