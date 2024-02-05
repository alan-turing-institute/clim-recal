FROM rocker/rstudio:4.3.2

# The local_data_path is an absolute local path to ClimateData on the machine hosting running `docker`
ARG HOST_DATA_PATH=/Volumes/vmfileshare

# The local_data_path is an absolute path to mount ClimateData within `docker`
ARG DOCKER_DATA_PATH=/mnt/vmfileshare

# Potential library versions/defaults to customise in future
# ENV S6_VERSION=v2.1.0.2
# ENV RSTUDIO_VERSION=2023.09.1+494
# ENV DEFAULT_USER=rstudio
# ENV PANDOC_VERSION=default
# ENV QUARTO_VERSION=default

RUN /rocker_scripts/install_pandoc.sh
# RUN /rocker_scripts/install_quarto.sh
# RUN /rocker_scripts/install_jupyter.sh

# Maybe install necessary geo packages for terra following https://github.com/rspatial/terra/issues/248
# RUN apt-get update && apt-get -y install libudunits2-dev \
#     libgeos-dev libproj-dev python3-gdal libgdal-dev \
#     build-essential
# Python specific deps

# For future: add dependencies to include to speed up notebook deploy, including
# library(ggplot2)
# library(terra)
# library(tmap) #pretty maps
# library(RColorBrewer)
# library(tidyverse)
# library(kableExtra)
# install.packages("hydroGOF")

COPY . /home/rstudio/

# If using outside docker compose, port may need to be exposed
# EXPOSE 8787

CMD ["/init"]
