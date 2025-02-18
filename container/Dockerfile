FROM mambaorg/micromamba:latest

WORKDIR /home/mambauser

COPY --chown=$MAMBA_USER:$MAMBA_USER . /ingest-adapter/

RUN micromamba install -y -n base -f /opera-rtc-s1-browse/environment.yml && \
    micromamba install -y -n base git && \
    micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1
RUN python -m pip install -e /ingest-adapter/

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python", "-m", "ingest_adapter.main"]
