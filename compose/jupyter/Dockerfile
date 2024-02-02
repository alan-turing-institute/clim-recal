FROM quay.io/jupyter/r-notebook

ARG env_name=clim-recal
ENV env_name=$env_name
ARG py_ver=3.10

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare

USER root
RUN apt-get update && apt-get -y install gdal-bin python3-gdal libgdal-dev build-essential git && \
    rm -rf /var/lib/apt/lists/*

RUN git config --global --add safe.directory /tmp

COPY --chown=${NB_UID}:${NB_GID} environment.yml /tmp/
RUN mamba env create -p "${CONDA_DIR}/envs/${env_name}" -f /tmp/environment.yml && \
    mamba clean --all -f -y

COPY . .
# Create Python kernel and link it to jupyter
RUN "${CONDA_DIR}/envs/${env_name}/bin/python" -m ipykernel install --user --name="${env_name}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

RUN apt-get update && apt-get -y install gdal-bin python3-gdal libgdal-dev build-essential git && \
    rm -rf /var/lib/apt/lists/*
RUN \
    # This changes a startup hook, which will activate the custom environment
    echo conda activate "${env_name}" >> /usr/local/bin/before-notebook.d/10activate-conda-env.sh && \
    # This makes the custom environment default in Jupyter Terminals for all users which might be created later
    echo conda activate "${env_name}" >> /etc/skel/.bashrc && \
    # This makes the custom environment default in Jupyter Terminals for already existing NB_USER
    echo conda activate "${env_name}" >> "/home/${NB_USER}/.bashrc"

USER ${NB_UID}
