---
title: "`clim-recal` installation"
---

`clim-recal` is a set of tools to manage UK climate data and projections, and running a range of correction methods in `python` and `R`. For ease of installation and use, we focus primarily on the `python` portion, but in principal the [`R` notebooks](R/) should be useable via `RStudio`, which suggests packages necessary to install. We also provide [`docker`](https://www.docker.com/) installation options which include both `R` and `python` dependencies.

# Quickstart

For users who already have `conda` (or `mamba`) installed:

```bash
git clone https://github.com/alan-turing-institute/clim-recal
cd clim-recal
pip install conda-lock
conda-lock install --name clim-recal conda-lock.yml
conda activate clim-recal
cd python
pdm install
clim-recal --help
```

For `docker`:

```bash
git clone https://github.com/alan-turing-institute/clim-recal
cd clim-recal
docker compose build jupyter
docker compose up jupyter
docker compose run jupyter bash
cd python
pdm install
pdm run clim-recal --help
```
::: {.callout-warning}
There are cases where `pdm install` raises a `KeyError: '_PYPROJECT_HOOKS_BUILD_BACKEND'`. Thus far, running `pdm install` again works.
:::

```console
clim-recal --help

```

# Python

## `Conda` / `Mamba`

At present either `conda` or `mamba` are needed to use `clim-recal`. Installation instructions for these are available:

- `conda`: <https://conda.io/projects/conda/en/latest/user-guide/install/index.html>
- `mamba`: <https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html>


<!--
We support two main options for installing the `python` components:
    - [`conda-lock`](https://conda.github.io/conda-lock/)
    - [`pip`](https://pdm-project.org/latest/)
[`conda-lock`](https://conda.github.io/conda-lock/) is the most comprehensive configuration, including geospatial elements.
 `pip` (or `pipx`) are enough to manage some of the basic `python` elements, but not the geospatial components.
-->
## `conda-lock`

`conda-lock` provides a means of managing dependencies for `conda`/`mamba` environments on precise, reproducible levels, and can incorporated local `pyproject.toml` configurations.

There are many ways to install `conda-lock` (for most up to date options see [`README.md`](https://github.com/conda/conda-lock?tab=readme-ov-file#installation)):


::: {.panel-tabset}

## `pipx`

```bash
pipx install conda-lock
```

## `pip`

```bash
pip install conda-lock
```

## `conda`

```bash
conda install --channel=conda-forge --name=base conda-lock
```

## `mamba`
```bash
mamba install --channel=conda-forge --name=base conda-lock
```

:::

Once `conda-lock` is installed, `clim-recal` can be installed via

::: {.panel-tabset}

## `pipx`

```bash
cd `clim-recal`
conda-lock install conda-lock.yml
```

## `pip`

```bash
cd `clim-recal`
conda-lock install conda-lock.yml
```

## `conda`

```bash
cd `clim-recal`
conda activate base
conda-lock install conda-lock.yml
```

## `mamba`

```bash
cd `clim-recal`
mamba activate base
conda-lock install conda-lock.yml
```

:::

# Docker

It can take some time to build, but the following will build 3 `docker` `volumes` for

- `jupyter`: a `python` environment
- `RServer`: an `R` environment
- `quarto`: documentation

::: {.panel-tabset}

## All

```bash
cd clim-recal
docker compose build
```

## `Jupyter` (`python` specific)

```bash
cd clim-recal
docker compose build jupyter
```

## `RStudio` (`R` specific)

```bash
cd clim-recal
docker compose build rstudio
```

## `quarto` (documentation)

```bash
cd clim-recal
docker compose build quarto
```

:::

Once built, the environments built can be run via

::: {.panel-tabset}

## All

```bash
docker compose up
```

## `Jupyter`

```bash
docker compose jupyter up
```

## `RStudio`

```bash
docker compose rstudio up
```

## `quarto`

```bash
docker compose quarto up
```

:::
