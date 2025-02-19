# ingest-adapter

An application for publishing HyP3 output products for ingest into ASF's data catalog. 
It contains a hyp3 plugin and an application for publishing jobs sent to it from the hyp3 plugin.

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
        - TOPIC_ARN
        - HYP3_URL
```

## Developer Setup

To run all commands in sequence use:
```bash
git clone https://github.com/ASFHyP3/ingest-adapter.git
mamba env create -f environment.yml
export PYTHONPATH=.... # TODO: What to add to python path
pytest tests
```
