---
title: "Contributing to our project"
---

We welcome contributions to our repository and follow the [Turing Way Contributing Guidelines](https://github.com/the-turing-way/the-turing-way/blob/main/CONTRIBUTING.md). As the project develops we will expand this section with details more specific to our code base and potential use/application.

# Documentation

Our documentation (including this file) is written in `md` and `qmd` file formats stored in the `docs` folder of our `GitHub` repository. To render those files we use [`quarto`](https://quarto.org).

## Running Quarto Locally

If you would like to render documentation locally you can do so via a [`conda`](https://docs.conda.io) or [`docker`](https://docker.com)

We appreciate your patience and encourage you to check back for updates on our ongoing documentation efforts.

### Locally via `conda`

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

### Locally via `docker`

If you have [`docker`](docker.com) installed, you can run a version of the `jupyter` conifiguration and `quarto`. The simplest and quickest solution, assuming you have `docker` running, is:

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

Currently, only the `python` portions of `clim-recal` have unit tests, and some of those require direct access to `ClimateData` mounted on `/mnt/vmfileshare/ClimateData` (which matches our configuration on `linux`). There are ways of running those tests locally if you are able to mount the `ClimateData` drive to that path, either via `conda` or `docker` (`conda` if running `linux`, in theory any operating system if running `docker`). We will expand the details of this process in future, but for tests that do not require `ClimateData`, the instructions for running those are below:

## Running tests in `conda`

Once the `conda` environment is installed, it should be straightforward to run the basic tests. The following example starts from a fresh clone of the repository:

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ conda create -n clim-recal -f environment.yml
$ conda activate clim-recal
$ cd python         # Currently the tests must be run within the `python` folder
$ pytest
Test session starts (platform: linux, Python 3.9.18, pytest 7.4.3, pytest-sugar 0.9.7)
rootdir: code/clim-recal/python          # Path printed is tweaked here for convenience
configfile: .pytest.ini
testpaths: tests, utils.py
plugins: cov-4.1.0, sugar-0.9.7

 tests/test_debiasing.py ✓✓✓sss✓✓✓✓✓✓✓✓sss✓                                  82% ████████▎
 utils.py ✓✓✓✓                                                              100% ██████████
Saved badge to clim-recal/python/docs/assets/coverage.svg

---------- coverage: platform linux, python 3.9.18-final-0 -----------
Name                                 Stmts   Miss  Cover
--------------------------------------------------------
conftest.py                             32      4    88%
data_download/ceda_ftp_download.py      59     59     0%
debiasing/preprocess_data.py           134    134     0%
debiasing/run_cmethods.py              108    108     0%
load_data/data_loader.py                83     83     0%
resampling/check_calendar.py            46     46     0%
resampling/resampling_hads.py           59     59     0%
tests/test_debiasing.py                188     27    86%
--------------------------------------------------------
TOTAL                                  732    520    29%

5 files skipped due to complete coverage.

=========================== short test summary info ============================
SKIPPED [1] <doctest test_debiasing.RunConfig.mod_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.obs_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.preprocess_out_path[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.yield_obs_folder[0]>:2: requires linux server mount paths
SKIPPED [1] <doctest test_debiasing.RunConfig.yield_preprocess_out_folder[0]>:2: requires linux server mount paths

Results (0.14s):
      16 passed
       6 skipped
       4 deselected
```

The `SKIPPED` messages of 6 [`doctests`](https://docs.python.org/3/library/doctest.html) show they are automatically skipped if the `linux server mount` is not found, specifically data to test in `/mnt/vmfileshare/ClimateData`.

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

To run tests that require mounting `ClimateData` (which are not enabled by default), you will need to have a local mount of the relevant drive. This is easiest to achieve by building the `compose/Dockerfile` separately (not using `compose`) with that drive mounted.

```sh
$ git clone https://github.com/alan-turing-institute/clim-recal
$ cd clim-recal
$ docker build -f compose/Dockerfile --tag 'clim-recal-test' .
$ docker run -it -p 8888:8888 -v /Volumes/vmfileshare:/mnt/vmfileshare clim-recal-test .
```

This will print information to the terminal including the link to the new `jupyter` session in this form:

```sh
[I 2023-11-16 13:46:31.350 ServerApp]     http://127.0.0.1:8888/lab?token=a-long-list-of-characters-to-include-in-a-url
```

By copying your equivalent of `http://127.0.0.1:8888/lab?token=a-long-list-of-characters-to-include-in-a-url` you should be able to get a `jupyer` instance with all necessary packages installed running in your browser.

From there, you can select a `Terminal` options under `Other` to get access to the terminal within your local `docker` build. You can then change to the `python` folder and run the tests with the `server` option to include `ClimateData` tests as well (note the `a-hash-sequence` will depend on your build):

```sh
(clim-recal) jovyan@a-hash-sequence:~$ cd python
(clim-recal) jovyan@a-hash-sequence:~/python$ pytest -m server
Test session starts (platform: linux, Python 3.9.18, pytest 7.4.3, pytest-sugar 0.9.7)
rootdir: /home/jovyan/python
configfile: .pytest.ini
testpaths: tests, utils.py
plugins: cov-4.1.0, sugar-0.9.7

 tests/test_debiasing.py ✓✓✓✓                         100% ██████████
Saved badge to /home/jovyan/python/docs/assets/coverage.svg

---------- coverage: platform linux, python 3.9.18-final-0 ---------
Name                                             Stmts   Miss  Cover
--------------------------------------------------------------------
conftest.py                                         32      4    88%
data_download/ceda_ftp_download.py                  59     59     0%
debiasing/preprocess_data.py                       134     21    84%
debiasing/python-cmethods/cmethods/CMethods.py     213    144    32%
debiasing/run_cmethods.py                          108      8    93%
load_data/data_loader.py                            83     83     0%
resampling/check_calendar.py                        46     46     0%
resampling/resampling_hads.py                       59     59     0%
tests/test_debiasing.py                            188      5    97%
utils.py                                            23      5    78%
--------------------------------------------------------------------
TOTAL                                              945    434    54%

5 files skipped due to complete coverage.


Results (955.60s (0:15:55)):
       4 passed
      22 deselected
```
