# FROM rocker/rstudio
#
# ENV LC_ALL en_GB.UTF-8
# ENV LANG en_GB.UTF-8
# ENV LANGUAGE en_GB.UTF-8
# ENV SHELL /bin/bash
# ARG env_name=clim-recal
#
# ARG py_ver=3.11
#
# # The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
# ARG local_data_path=/Volumes/vmfileshare/ClimateData
#
# # The local_data_path is an absolute path to mount ClimateData within `docker`
# ARG docker_data_path=/Volumes/vmfileshare/ClimateData
#
#
# USER root
#
# # Generate the locales
# RUN echo "en_GB.UTF-8 UTF-8" > /etc/locale.gen && locale-gen
#
#
# RUN apt-get update && apt-get -y install gdal-bin python3-gdal libgdal-dev build-essential
#
# # Ensure correct GDAL paths
# RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal && export C_INCLUDE_PATH=/usr/include/gdal
#
# # Switch to default jupyter user
# USER ${NB_UID}
#
# ENTRYPOINT ['bash']
ARG QUARTO_VERSION="1.3.450"

FROM ghcr.io/quarto-dev/quarto:${QUARTO_VERSION} AS builder

ARG PORT=8080
ARG py_ver=3.9

# ARG RIG_VERSION="latest"
# ARG R_VERSION="release"
# COPY install-rig.sh /tmp/install-rig.sh
# RUN bash /tmp/install-rig.sh "${RIG_VERSION}"
# RUN rig add ${R_VERSION} # && Rscript -e 'pak::pkg_install("renv")'

# COPY mywebsite /app
# WORKDIR /app
# RUN Rscript -e "renv::restore()"
# RUN quarto render .
ADD . /app
WORKDIR /app
# RUN Rscript -e "renv::restore()"
EXPOSE ${PORT}:${PORT}

# RUN quarto preview --port ${PORT}:${PORT}
RUN apt-get update && apt-get install -y python${py_ver} python3-pip
RUN pip3 install quartodoc && quartodoc build
RUN quarto render

FROM httpd:alpine
COPY --from=builder /app/_site/ /usr/local/apache2/htdocs/
