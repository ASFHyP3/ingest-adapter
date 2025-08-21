class SkipIngestError(Exception):
    """Raised when skipping ingest for a given job because it does not meet the requirements for that job type."""
