# ingest-adapter

An application for publishing HyP3 output products for ingest into ASF's data catalog. 
It contains a HyP3 plugin and an application for publishing jobs sent to it from the HyP3 plugin.

Support is currently limited to:
- Publishing the outputs of `ARIA_S1_GUNW` jobs to the `ARIA_S1_GUNW` CMR collection
- Publishing the outputs of `OPERA_RTC_S1_SLC` jobs to the `OPERA_L2_RTC-S1_V1` CMR collection

## Usage

To allow for a HyP3 job to publish its outputs, use the ingest-adapter container and add it as a step to the job spec file
that looks like this:

```yaml
- name: PUBLISH
  image: ghcr.io/asfhyp3/ingest-adapter
  command:
    - Ref::job_id
  timeout: 600
  compute_environment: Default
  vcpu: 1
  memory: 512
  secrets:
    - TOPIC_ARN
    - HYP3_URL
```

And add these parameters to the Secrets Manager secret for that particular HyP3 deployment:
- `TOPIC_ARN`: the SNS Topic for the desired ingest-adapter deployment
- `HYP3_URL`: the api URL for that particular HyP3 deployment

## Developer Setup

To run all commands in sequence use:

```bash
mamba env create -f environment.yml
mamba activate ingest-adapter
export PYTHONPATH=${PWD}/app/src:${PWD}/plugin/src
pytest tests
```
