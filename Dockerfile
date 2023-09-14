FROM jupyter/r-notebook

ENV LC_ALL en_GB.UTF-8
ENV LANG en_GB.UTF-8
ENV LANGUAGE en_GB.UTF-8
ENV SHELL /bin/bash
# ENV CONDA_DIR /usr/lib
ARG env_name=clim-recal
ARG py_ver=3.11

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
    # mamba install --yes 'jupyterlab' 'notebook' 'jupyterhub' 'nbclassic' 'ipykernel' && \ 
    mamba clean --all -f -y

# Any additional `pip` installs can be added by using the following line
# Using `mamba` is highly recommended though
RUN "${CONDA_DIR}/envs/${env_name}/bin/pip" install --no-cache-dir \
    'ipykernel'

# Create kernel from custome environment.yml
RUN "${CONDA_DIR}/envs/${env_name}/bin/python" -m ipykernel install --user --name="${env_name}" && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"

# Copy the rest of the  clim-recal code to volume
COPY --chown=${NB_UID}:${NB_GID} . .

# Add custom activate script to reflect environment
USER root
RUN activate_custom_env_script=/usr/local/bin/before-notebook.d/activate_custom_env.sh && \
    echo "#!/bin/bash" > ${activate_custom_env_script} && \
    echo "eval \"$(conda shell.bash activate "${env_name}")\"" >> ${activate_custom_env_script} && \
    chmod +x ${activate_custom_env_script}


USER ${NB_UID}

# Set this for default conda activate config
# You can comment this line to keep the default environment in Terminal
RUN echo "conda activate ${env_name}" >> "${HOME}/.bashrc"
