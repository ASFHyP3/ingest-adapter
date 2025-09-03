from enum import StrEnum
from typing import TypedDict


class Collection(StrEnum):
    ARIA_S1_GUNW = 'ARIA_S1_GUNW'
    OPERA_RTC_S1_SLC = 'OPERA_L2_RTC-S1_V1'


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
    collection: Collection
    version: str
    submissionTime: str
    product: IngestProduct
    provider: str
    trace: str
