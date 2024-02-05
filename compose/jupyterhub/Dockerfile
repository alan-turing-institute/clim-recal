# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
ARG JUPYTERHUB_VERSION
FROM quay.io/jupyterhub/jupyterhub:$JUPYTERHUB_VERSION

ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare
ARG env_name=clim-recal

# Install dockerspawner, nativeauthenticator
# hadolint ignore=DL3013
RUN python3 -m pip install --no-cache-dir \
    dockerspawner \
    jupyterhub-nativeauthenticator


COPY . .
WORKDIR python/debiasing/ && git submodule update --init --recursive
WORKDIR $HOME

CMD jupyterhub -f /srv/jupyterhub/jupyterhub_config.py

# TODO: test uncommenting to add necessary packages in future
# RUN conda update -n base -c conda-forge conda
# RUN conda env create -f environment.yml -n clim-recal
