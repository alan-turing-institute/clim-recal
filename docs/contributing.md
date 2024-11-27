---
title: "Contributing to clim-recal"
---

We welcome contributions to our repository and follow the [Turing Way Contributing Guidelines](https://github.com/the-turing-way/the-turing-way/blob/main/CONTRIBUTING.md).

# Documentation

Our documentation (including this file) is written in `md` and `qmd` file formats stored in the `docs` folder of our `GitHub` repository. To render those files we use [`quarto`](https://quarto.org).

## Running Quarto Locally

If you would like to render documentation locally you can do so via a [`conda`](https://docs.conda.io) or [`docker`](https://docker.com)

### Running Quarto locally via `conda`

1. Ensure you have a [local installation](https://conda.io/projects/conda/en/latest/user-guide/install/index.html) of `conda` or [`anaconda`](https://www.anaconda.com/download) .
1. Checkout a copy of our `git` repository
1. Create a local `conda` `environment` via our `environment.yml` file. This should install `quarto`.
1. Activate that environment
1. Run `quarto preview`.

Below are example `bash` shell commands to render locally after installing `conda`:

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ conda create -n clim-recal -f environment.yml
$ conda activate clim-recal
$ quarto preview
```

### Running Quarto locally via `docker`

If you have `docker` installed, you can run a version of the `jupyter` configuration and `quarto`. The simplest and quickest solution, assuming you have `docker` running, is:

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ docker compose build
$ docker compose up
```

This should generate local sessions of:

- `jupyter` for the `python/` model: <http://localhost:8888>
- `quarto` documentation: <http://localhost:8080>

# GitHub Actions Continuous Integration

We have set up `GitHub` `Actions` continuous integration to test `pull` requests to our `GitHub` repository <https://github.com/alan-turing-institute/clim-recal>. Please create a `branch` and [pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request) and relevant ticket for contributions (more details on this to come).

# Linting `git` commits

Assuming you have a local `clim-recal` `checkout`, you will need to install and enable `pre-commit` to check commits prior to `pull` `requests`. If you're using `conda` that should include a `pre-commit` install.

```sh
(clim-recal)$ cd clim-recal
(clim-recal)$ git pre-commit install                # Enables pre-commit for git
```

If you aren't using `conda` (and if so you can try `docker`), you can install a local copy of `pre-commit` via `python` `pip`

```sh
$ pip3 install pre-commit    # Or depending on your python install: `pip install pre-commit`
$ cd clim-recal
$ git pre-commit install    # Enables pre-commit for git
```

However `pre-commit` is enabled with your `clim-recal` `git` checkout, below is an example of how it checks changes:

```sh
(clim-recal)$ git checkout -b fix-doc-issue         # fix-doc-issue is new branch name
(clim-recal)$ git add docs/contributing.md          # Add changed file
(clim-recal)$ git commit -m "fix(doc): typo in docs/contributing.md, closes #99"  # Reference the issue addressed
black-jupyter............................................................Passed
check for added large files..............................................Passed
check for case conflicts.................................................Passed
check for merge conflicts................................................Passed
check for broken symlinks............................(no files to check)Skipped
check yaml...............................................................Passed
debug statements (python)................................................Passed
fix end of files.........................................................Passed
mixed line ending........................................................Passed
fix requirements.txt.................................(no files to check)Skipped
trim trailing whitespace.................................................Passed
rst ``code`` is two backticks........................(no files to check)Skipped
rst directives end with two colons...................(no files to check)Skipped
rst ``inline code`` next to normal text..............(no files to check)Skipped
pycln....................................................................Passed
isort (python)...........................................................Passed
[fix-doc-issue 82tq03n] fix(doc): typo in `docs/contributing.md`, closes #99
 1 file changed, 1 insertion(+), 1 deletion(-)
```

# Running tests

The python `clim-recal` codebase includes a mixture of unit and integration tests.

The integration tests are identified using the pytest markers specified in the `markers` section of `pyproject.toml`. These marker description indicate the environment required to run each set of integration test. For example where a particular operating system is required, or for case which require direct access to [the data](/README#the-datasets) mounted on the hardcoded path `/mnt/vmfileshare/ClimateData`.

The instructions below cover running the tests on a development machine which do not require access to the external data:

## Running tests in `conda`

Once the `conda` environment is installed, it should be straightforward to run the basic tests. The following example starts from a fresh clone of the repository:

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ conda create -n clim-recal -f environment.yml
$ conda activate clim-recal
$ cd python         # Currently the tests must be run within the `python` folder
$ pytest
```

## Running tests in Docker

With a `docker` install, tests can be run in two ways. The simplest is via `docker compose`:

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ docker compose build
$ docker compose up
$ docker compose exec jupyter bash -c "conda run -n clim-recal --cwd python pytest"
```

This mirrors the way tests are run via `GitHub` `Actions` for continuous integration on <https://github.com/alan-turing-institute/clim-recal>.
