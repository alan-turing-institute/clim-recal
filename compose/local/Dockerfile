FROM jupyter/r-notebook as clim-recal-base

# This is derived from documentation available at
# https://jupyter-docker-stacks.readthedocs.io/en/latest/

# Example run command:


# This will require a mount of `vmfileshare` from `dymestorage1`
# On macOS this can be solved via:
# open smb://dymestorage1.file.core.windows.net/vmfileshare
# Using user: dymestorage1
# And password specified via:
# https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage?tabs=azure-portal#view-account-access-keys

# Example run:
# cd clim-recal
# docker build --tag 'clim-recal' .
# docker run -it -p 8888:8888 -v /Volumes/vmfileshare:/home/jovyan/work/vmfileshare clim-recal

ENV LC_ALL en_GB.UTF-8
ENV LANG en_GB.UTF-8
ENV LANGUAGE en_GB.UTF-8
ENV SHELL /bin/bash
ARG env_name=clim-recal

# `py_ver` is not currently used below and is specified in `environment.yaml`
# here as reminder and clarity if future change needed.
ARG py_ver=3.11

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare


USER root

# Generate the locales
RUN echo "en_GB.UTF-8 UTF-8" > /etc/locale.gen && locale-gen


RUN apt-get update && apt-get -y install gdal-bin python3-gdal libgdal-dev build-essential
RUN conda update -n base -c conda-forge conda

# Ensure correct GDAL paths
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal && export C_INCLUDE_PATH=/usr/include/gdal

# Create custom environment from environment.yml
# Add ipykernel for environment build as necessary
COPY --chown=${NB_UID}:${NB_GID} environment.yml /tmp/
RUN mamba env create -p "${CONDA_DIR}/envs/${env_name}" -f /tmp/environment.yml && \
    mamba clean --all -f -y

# Any additional `pip` installs can be added by using the following line
# Using `mamba` is highly recommended though
RUN "${CONDA_DIR}/envs/${env_name}/bin/pip" install --no-cache-dir \
    'ipykernel'

# Create kernel from custome `environment.yml`
RUN "${CONDA_DIR}/envs/${env_name}/bin/python" -m ipykernel install --user --name="${env_name}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# Copy the rest of the  clim-recal code to volume
ADD --chown=${NB_UID}:${NB_GID} . .


# Add custom activate script to reflect environment
USER root
RUN activate_custom_env_script=/usr/local/bin/before-notebook.d/activate_custom_env.sh && \
    echo "#!/bin/bash" > ${activate_custom_env_script} && \
    echo "eval \"$(conda shell.bash activate "${env_name}")\"" >> ${activate_custom_env_script} && \
    chmod +x ${activate_custom_env_script}

# Switch to default jupyter user
USER ${NB_UID}

# Set this for default `conda activate` configuration
# You can comment this line to keep the default environment in Terminal
RUN echo "conda activate ${env_name}" >> "${HOME}/.bashrc"

RUN cd python/debiasing && git submodule update --init --recursive


# This will use the default launch as discussed in
# https://jupyter-docker-stacks.readthedocs.io/en/latest/using/running.html

# FROM clim-recal-base as clim-recal-docs
#
# ARG port=4200
#
# USER root
#
# RUN sudo apt-get -y install \
#     pandoc \
#     pandoc-citeproc \
#     curl \
#     gdebi-core \
#     r-base \
#     r-base-dev \
#     r-cran-littler \
#     && rm -rf /var/lib/apt/lists/*
#
# # RUN Rscript -e \
# # "install.packages(c('shiny', 'jsonlite', 'renv', 'knitr', 'rmarkdown', 'quarto'), repos='https://cran.rstudio.com')"
# SHELL ["/bin/bash", "-o", "pipefail", "-c"]
#
# RUN arch=$(arch | sed s/aarch64/arm64/ | sed s/x86_64/amd64/) && \
#   curl -LO https://quarto.org/download/latest/quarto-linux-${arch}.deb && \
#   gdebi --non-interactive quarto-linux-${arch}.deb
#
# #    wget "http://...../${arch}.deb"
#
# # RUN curl -LO https://quarto.org/download/latest/quarto-linux-amd64.deb
# # RUN gdebi --non-interactive quarto-linux-${arch}.deb
#
# EXPOSE ${port}:${port}
#
# USER ${NB_UID}
#
# # CMD ["quarto", "preview", "--port={port}"]
# CMD ["bash"]
