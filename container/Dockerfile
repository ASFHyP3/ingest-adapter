FROM mambaorg/micromamba:latest

WORKDIR /home/mambauser

COPY --chown=$MAMBA_USER:$MAMBA_USER . /opera-rtc-s1-browse/

RUN micromamba install -y -n base -f /opera-rtc-s1-browse/environment.yml && \
    micromamba install -y -n base git && \
    micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1
RUN python -m pip install -e /opera-rtc-s1-browse/

ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "python", "-m", "opera_rtc_s1_browse.harmony_service"]
