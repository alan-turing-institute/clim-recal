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
