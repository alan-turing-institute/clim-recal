FROM quay.io/jupyter/r-notebook

ARG env_name=clim-recal
ENV env_name=$env_name

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare

USER ${NB_UID}
COPY --chown=$NB_USER:$NB_USER conda-lock.yml /tmp/conda-lock.yml

RUN conda install -y conda-lock

RUN \
  conda-lock install --copy -n ${env_name} /tmp/conda-lock.yml && \
  conda clean --all -y && \
  fix-permissions "${CONDA_DIR}" && \
  fix-permissions "/home/${NB_USER}"

# Note the below originally was run just prior to `conda clean`
# but may no longer be needed
# conda run -n ${env_name} python -m ipykernel install --user --name ${env_name} && \

# Needed for write permission
USER root

# First line changes a startup hook, which will activate the custom environment
RUN \
    echo conda activate "${env_name}" >> /usr/local/bin/before-notebook.d/10activate-conda-env.sh && \
    echo conda activate "${env_name}" >> /etc/skel/.bashrc
# Above line makes the custom environment default in Jupyter Terminals for all users which might be created later

RUN chown -R ${NB_USER} /home/${NB_USER}

USER ${NB_UID}

COPY . .

# This makes the custom environment default in Jupyter Terminals for already existing NB_USER
# Note this *doesn't* apply if simply using sh -c
RUN echo conda activate "${env_name}" >> "/home/${NB_USER}/.bashrc"
