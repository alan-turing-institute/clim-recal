ARG QUARTO_VERSION="1.3.450"

FROM registry.gitlab.com/quarto-forge/docker/polyglot as builder

ARG LOCAL_PORT=8080
ARG EXTERNAL_PORT=$LOCAL_PORT

# Setting as an ENV allows LOCAL_PORT to be used in
# runtime commandas like CMD and still be passed as
# an ARG
ENV HOST_IP=0.0.0.0
ENV LOCAL_PORT=${LOCAL_PORT}
ARG py_ver=3.11
ENV DEBIAN_FRONTEND=noninteractive

COPY . /app
WORKDIR /app

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare

USER root

# Install the local python environment set via
# `pre-commit` in .conda-linux-64
RUN micromamba create -f .conda-linux-64.lock -n clim-recal


CMD micromamba run -n clim-recal quartodoc build && \
    micromamba run -n clim-recal quarto preview --port $LOCAL_PORT --host $HOST_IP
