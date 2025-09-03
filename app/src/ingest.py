from typing import TypedDict


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
