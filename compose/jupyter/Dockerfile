FROM quay.io/jupyter/r-notebook

ARG env_name=clim-recal
ENV env_name=$env_name

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare


# Needed for write permission
USER root

# Build-essential for gdal and potentially packages like scipy
RUN sudo apt-get update && sudo apt-get install -y build-essential

# First line changes a startup hook, which will activate the custom environment
# RUN echo conda activate "${env_name}" >> /usr/local/bin/before-notebook.d/10activate-conda-env.sh

COPY . .

RUN chown -R ${NB_USER}:${NB_GID} /home/${NB_USER}

USER ${NB_USER}

RUN conda create -n ${env_name}
# Todo: either return to an update to environment.yml or document this
RUN conda install -n ${env_name} -y gdal=3.8.5 pdm rsync

# This makes the custom environment default in Jupyter Terminals for already existing NB_USER
# Note this *doesn't* apply if simply using sh -c
RUN echo conda activate "${env_name}" >> "/home/${NB_USER}/.bashrc"

# Install dependencies
RUN cd python && conda run -n ${env_name} pdm install
