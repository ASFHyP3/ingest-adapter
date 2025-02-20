# ingest-adapter

An application for publishing HyP3 output products for ingest into ASF's data catalog. 
It contains a HyP3 plugin and an application for publishing jobs sent to it from the HyP3 plugin.

## Usage

To allow for a HyP3 job to publish its outputs, use the ingest-adapter container and add it as a step to the job_spec file
that looks like this:

```yaml
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

## Developer Setup

To run all commands in sequence use:
```bash
mamba env create -f environment.yml
export PYTHONPATH=${PWD}/app/src:${PWD}/plugin/src
pytest tests
```
