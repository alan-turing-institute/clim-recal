ARG QUARTO_VERSION="1.3.450"

# FROM ghcr.io/quarto-dev/quarto:${QUARTO_VERSION} AS builder
FROM registry.gitlab.com/quarto-forge/docker/polyglot as builder

ARG PORT=8080
ARG py_ver=3.10
ENV DEBIAN_FRONTEND=noninteractive

# ARG RIG_VERSION="latest"
# ARG R_VERSION="release"
# COPY install-rig.sh /tmp/install-rig.sh
# RUN bash /tmp/install-rig.sh "${RIG_VERSION}"
# RUN rig add ${R_VERSION} # && Rscript -e 'pak::pkg_install("renv")'

# COPY mywebsite /app
# WORKDIR /app
# RUN Rscript -e "renv::restore()"
# RUN quarto render .
COPY . /app
WORKDIR /app
# RUN Rscript -e "renv::restore()"
EXPOSE ${PORT}:${PORT}

# RUN quarto preview --port ${PORT}:${PORT}
# RUN apt-get update && apt-get install -y python${py_ver} python3-pip r-base r-base-dev
# RUN micromamba install -y -n base quartodoc && quartodoc build

RUN micromamba config append channels conda-forge && \
    micromamba install -y -n base quartodoc


USER root

# RUN micromamba run -n base quartodoc build && \
#     micromamba run -n base quarto render

RUN micromamba run -n base quarto render

# RUN quartodoc build

# RUN Rscript -e 'install.packages("rmarkdown", repos="https://cloud.r-project.org")'

# RUN micromamba run -n base quarto render

FROM httpd:alpine
COPY --from=builder /app/_site/ /usr/local/apache2/htdocs/
