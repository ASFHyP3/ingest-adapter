# ingest-adapter
An application for publishing HyP3 output products for ingest into ASF's data catalog

## Developer Setup

### App Setup

1. Ensure that conda is installed on your system (we recommend using [mambaforge](https://github.com/conda-forge/miniforge#mambaforge) to reduce setup times).
2. Download a local version of the `ingest-adapter` repository (`git clone https://github.com/ASFHyP3/ingest-adapter.git`)
3. In the base directory for this project call `mamba env create -f environment.yml` to create your Python environment, then activate it (`mamba activate ingest-adapter`)
4. Finally, install a development version of the package (`python -m pip install -e .`)

To run all commands in sequence use:
```bash
git clone https://github.com/ASFHyP3/ingest-adapter.git
cd ingest-adapter
mamba env create -f environment.yml
mamba activate ingest-adapter
python -m pip install -e .
```

### Container Setup

To install the container run the following commands in the root of the repository

```bash
cd container?
mamba env create -f environment.yml
mamba activate container?
python -m pip install -e .
```

## Usage

To allow for a HyP3 job to publish it's outputs, use the ingest-adapter container plugin and add it as a step to the job_spec file
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

To use the plugin directly to publish a job

```
ingest_adapter https://hyp3-api.asf.alaska.edu (job_id)
```
