---
title: "`clim-recal` installation"
---

`clim-recal` is a set of tools to manage UK climate data and projections, and running a range of correction methods in `python` and `R`. For ease of installation and use, we focus primarily on the `python` portion, but in principal the [`R` notebooks](R/) should be useable via `RStudio`, which suggests packages necessary to install. We also provide [`docker`](https://www.docker.com/) installation options which include both `R` and `python` dependencies.

# Quickstart

> [!WARNING]
There are several different ways to install `clim-recal` .
We have used docker the most and hence this is the most tested.
Other methods are available but may not be as well tested.

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
clim-recal --help
```
::: {.callout-warning}
There are cases where `pdm install` raises a
`KeyError: '_PYPROJECT_HOOKS_BUILD_BACKEND'`. Thus far, running `pdm install`
again works.
:::

# Options

Once installed the `clim-recal` command can be run with files. The settings
below are specific to a server

```bash
clim-recal --help

 Usage: clim-recal [OPTIONS]

 Crop and align UK climate projections and test debias methods.

╭─ Options ──────────────────────────────────────────────────────────────╮
│ --hads-input-path     -d      PATH                [default: .]         │
│ --cpm-input-path      -o      PATH                [default: .]         │
│ --output-path         -o      DIRECTORY           [default:            │
│                                                   clim-recal-runs]     │
│ --variable            -v      [tasmax|rainfall|t  [default: tasmax]    │
│                               asmin]                                   │
│ --region              -a      [Glasgow|Mancheste  [default:            │
│                               r|London|Scotland]  Manchester]          │
│ --run                 -r      [01|04|05|06|07|08  [default: 05]        │
│                               |09|10|11|12|13|14                       │
│                               |15]                                     │
│ --method              -m      [quantile_delta_ma  [default:            │
│                               pping|quantile_map  quantile_delta_mapp… │
│                               ping|variance_scal                       │
│                               ing|delta_method]                        │
│ --all-variables                                                        │
│ --all-regions                                                          │
│ --all-runs                                                             │
│ --default-runs                                                         │
│ --all-methods                                                          │
│ --project-cpm                                     [default: True]      │
│ --project-hads                                    [default: True]      │
│ --crop-cpm                                        [default: True]      │
│ --crop-hads                                       [default: True]      │
│ --execute                                                              │
│ --start-index         -s      INTEGER RANGE       [default: 0]         │
│                               [x>=0]                                   │
│ --total-from-index    -t      INTEGER RANGE       [default: 0]         │
│                               [x>=0]                                   │
│ --cpus                        INTEGER RANGE       [default: 2]         │
│                               [1<=x<=10]                               │
│ --use-multiprocessi…                                                   │
│ --install-completion                              Install completion   │
│                                                   for the current      │
│                                                   shell.               │
│ --show-completion                                 Show completion for  │
│                                                   the current shell,   │
│                                                   to copy it or        │
│                                                   customize the        │
│                                                   installation.        │
│ --help                                            Show this message    │
│                                                   and exit.            │
╰────────────────────────────────────────────────────────────────────────╯
```

# Conda/Mamba Python

## `Conda` / `Mamba`

More detailed examples using `conda` or `mamba` are below. Installation instructions for either these are available:

- `conda`: <https://conda.io/projects/conda/en/latest/user-guide/install/index.html>
- `mamba`: <https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html>

These options are primarily to ease use of `GDAL` and optionally `rsync`.

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
