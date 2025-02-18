# ingest-adapter
An application for publishing HyP3 output products for ingest into ASF's data catalog

## Usage

To allow for a HyP3 job to publish it's outputs, use the ingest-adapter container and add it as a step to the job_spec file
that looks like this:

```
      - name: PUBLISH
      image: ghcr.io/ASFHyP3/ingest-adapter
      command:
        - --job-id
        - Ref::bucket_prefix
      timeout: 600
      compute_environment: Default
      vcpu: 1
      memory: 512
      secrets:
        - INGEST_ADAPTER_SNS_TOPIC_ARN
        - HYP3_API_URL
```

To use the app directly to publish a job run

```
ingest_adapter https://hyp3-api.asf.alaska.edu (job_id)
```

## Developer Setup

To run all commands in sequence use:
```bash
git clone https://github.com/ASFHyP3/ingest-adapter.git
mamba env create -f environment.yml
export PYTHONPATH=....
pytest tests
```

## Deployment

TODO
