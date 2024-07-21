"""Wrappers to automate the entire pipeline.

Following Andy's very helpful `excel` file, this manages
a reproduction of all steps of the debiasing pipeline.

# Download Data

The `download_ftp` function from `ceda_ftp_download.py` can
be used (with registered account user name and password),
to download two datasets from the Centre for Environmental
Data Analysis (CEDA)

- Saved to `ClimateData/Raw`
- [`HadUK-Grid`](https://catalogue.ceda.ac.uk/uuid/e6822428e4124c5986b689a37fda10bc)
    - a 1km climate projection grid which is designed to supersede `UKCP`
    - For further details see [Met Office](https://www.metoffice.gov.uk/research/climate/maps-and-data/data/haduk-grid)
    - Saved to `Raw/HadsUKgrid/`
- [`UKCP`](https://catalogue.ceda.ac.uk/uuid/f9b6b55dfa174386a05efae2f0f76141) UK Climate Projections at 2.2 km
    - a 2.2km projection grid
    - Saved to `Raw/UKCP2.2/`

# Reproject UKCP

The `bash/reproject_one.sh` copies and reprojects `UKCP2.2`via `gdalwrap` to a `Raw/Reprojected_infill`:
```bash
gdalwarp -t_srs 'EPSG:27700' -tr 2200 2200 -r near -overwrite $f "${fn%.nc}.tif" # Reproject the file`
```

*New step*: project UKCP to 360/365 days

Relevant `xarray` utilities:

- [`convert_calendar`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.convert_calendar.html)
- [`interp_calendar`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.interp_calendar.html)

# Resample CPM

New approach:

- `resampling.py`
- check `x_grid` and `y_grid` interpolation

## Todo

- [x] Update the example here
- [x] Remove out of date elements
- [x] Hardcode the process below
- [x] Refactor to facilitate testing
- [x] Ensure HADs still works
- [x] Add function for UKCP
- [x] Check `convert_xr_calendar` `doctest` examples
- [ ] Fix order of UKCP changes

To run this step in the pipeline the following should work
for the default combindations of `variables`: `tasmax`,
`tasmin`, and `rainfall` and the default set of runs: `05`,
`06`, `07` and `08`, assuming the necessary data is mounted.

If installed via `pipx`/`pip` etc. on your local path (or within `Docker`)
clim-recal should be a command line function
```console
$ clim-recal --all-variables --default-runs --output-path /where/results/should/be/written
clim-recal pipeline configurations:
<ClimRecalConfig(variables_count=3, runs_count=4, regions_count=1, methods_count=1,
                 cpm_folders_count=12, hads_folders_count=3, start_index=0,
                 stop_index=None, cpus=2)>
<CPMResamplerManager(variables_count=3, runs_count=4, input_paths_count=12)>
<HADsResamplerManager(variables_count=3, input_paths_count=3)>
```
Otherwise, you can install locally and either run via
`pdm` from the `python` folder

```console
$ cd clim-recal/python
$ pdm run clim-recal --all-variables --default-runs --output-path /where/results/should/be/written
# Skipping output for brevity
```

Or within an `ipython` or `jupyter` instance (truncated below for brevity)

```python
>>> from clim_recal.pipeline import main
>>> main(all_variables=True, default_runs=True)  # doctest: +SKIP
clim-recal pipeline configurations:
<ClimRecalConfig(variables_count=3, runs_count=4, ...>
```

Regardless of your route, once you're confident with the
configuration, add the `--execute` parameter to run. For example,
assuming a local install:

```console
$ clim-recal --all-variables --default-runs --output-path /where/results/should/be/written --execute
clim-recal pipeline configurations:
<ClimRecalConfig(variables_count=3, runs_count=4, regions_count=1, methods_count=1,
                 cpm_folders_count=12, hads_folders_count=3, start_index=0,
                 stop_index=None, cpus=2)>
<CPMResamplerManager(variables_count=3, runs_count=4, input_paths_count=12)>
<HADsResamplerManager(variables_count=3, input_paths_count=3)>
Running CPM Standard Calendar projection...
<CPMResampler(count=100, max_count=100,
              input_path='/mnt/vmfileshare/ClimateData/Raw/UKCP2.2/tasmax/05/latest',
              output_path='/mnt/vmfileshare/ClimateData/CPM-365/test-run-3-may/resample/
              cpm/tasmax/05/latest')>
 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 100/100  [ 0:38:27 < 0:00:00 , 0 it/s ]
  87% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 87/100  [ 0:17:42 < 0:03:07 , 0 it/s ]
```

# Cropping


# Pre-processing

- Originally used `debiasing.pre_processing.py`

New approach:

- Refactor `debiasing.debias-wrapper`

# Debiasing

- `python`
    - Originally used `debiasing.pre_processing.py`
    - Refactor `debiasing.debias-wrapper`
- `R`

# Keep track

- What is being superseded
- What can be removed


"""

from os import PathLike
from pathlib import Path
from typing import Final, Sequence

from rich import print

from .config import (
    DEFAULT_OUTPUT_PATH,
    ClimRecalConfig,
    ClimRecalRunResultsType,
    MethodOptions,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .resample import CPMResampler, HADsResampler

REPROJECTION_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_one.sh")
REPROJECTION_WRAPPER_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_all.sh")


def main(
    execute: bool = False,
    output_path: PathLike = DEFAULT_OUTPUT_PATH,
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),),
    regions: Sequence[RegionOptions | str] | None = (RegionOptions.default(),),
    runs: Sequence[RunOptions | str] = (RunOptions.default(),),
    methods: Sequence[MethodOptions | str] = (MethodOptions.default(),),
    all_variables: bool = False,
    all_regions: bool = False,
    default_runs: bool = False,
    all_runs: bool = False,
    all_methods: bool = False,
    skip_cpm_standard_calendar_projection: bool = False,
    skip_hads_spatial_2k_projection: bool = False,
    skip_cropping: bool = True,
    crop_cpm: bool = False,
    crop_hads: bool = False,
    cpus: int | None = None,
    multiprocess: bool = False,
    start_index: int = 0,
    stop_index: int | None = None,
    total: int | None = None,
    print_range_length: int | None = 5,
    **kwargs,
) -> ClimRecalRunResultsType:
    """Run all elements of the pipeline.

    Parameters
    ----------
    variables
        Variables to include in the model, eg. `tasmax`, `tasmin`.
    runs
        Which model runs to include, eg. "01", "08", "11".
    regions
        Which regions to crop data to. Future plans facilitate
        skipping to run for entire UK.
    methods
        Which debiasing methods to apply.
    output_path
        `Path` to save intermediate and final results to.
    cpus
        Number of cpus to use when multiprocessing.
    multiprocess
        Whether to use multiprocessing.
    start_index
        Index to start all iterations from.
    total
        Total number of records to iterate over. 0 and
        `None` indicate all values from `start_index`.
    **kwargs
        Additional parameters to pass to a `ClimRecalConfig`.

    Notes
    -----
    The default parameters here are meant to reflect the entire
    workflow process to ease reproducibility.


    Examples
    --------

    Note the `_allow_check_fail` parameters support running
    the examples without data mounted from a server.

    >>> main(variables=("rainfall", "tasmin"),
    ...      output_path=test_runs_output_path,
    ...      cpm_kwargs=dict(_allow_check_fail=True),
    ...      hads_kwargs=dict(_allow_check_fail=True),
    ... )
    clim-recal pipeline configurations:
    <ClimRecalConfig(variables_count=2, runs_count=1,
                     regions_count=1, methods_count=1,
                     cpm_folders_count=2, hads_folders_count=2,
                     start_index=0, stop_index=None,
                     cpus=...)>
    <CPMResamplerManager(variables_count=2, runs_count=1,
                         input_paths_count=2)>
    <HADsResamplerManager(variables_count=2, input_paths_count=2)>
    No steps run. Add '--execute' to run steps.
    """
    if stop_index and total:
        print(
            f"Both 'stop_index': {stop_index} and 'total': {total} provided, skipping 'total'."
        )
    elif total:
        stop_index = None if total == 0 or total == None else start_index + total
        print(
            f"'stop_index': {stop_index} set from 'total': {total} and 'start_index': {start_index}."
        )
    variables = VariableOptions.all() if all_variables else tuple(variables)
    assert regions  # In future there will be support for skipping region cropping
    regions = RegionOptions.all() if all_regions else tuple(regions)
    methods = MethodOptions.all() if all_methods else tuple(methods)
    if all_runs:
        runs = RunOptions.all()
    elif default_runs:
        runs = RunOptions.preferred()
    else:
        runs = tuple(runs)

    config: ClimRecalConfig = ClimRecalConfig(
        output_path=output_path,
        variables=variables,
        regions=regions,
        methods=methods,
        runs=runs,
        cpus=cpus,
        multiprocess=multiprocess,
        start_index=start_index,
        stop_index=stop_index,
        **kwargs,
    )
    print("clim-recal pipeline configurations:")
    print(config)
    print(config.cpm_manager)
    print(config.hads_manager)
    if execute:
        if skip_cpm_standard_calendar_projection:
            print("Skipping CPM Strandard Calendar projection.")
        else:
            print("Running CPM Standard Calendar projection...")
            cpm_resamplers: tuple[CPMResampler, ...] = (
                config.cpm_manager.execute_resample_configs(
                    multiprocess=multiprocess, cpus=cpus
                )
            )
            print(cpm_resamplers[:print_range_length])
        if skip_hads_spatial_2k_projection:
            print("Skipping HADs aggregation to 2.2km spatial units.")
        else:
            print("Running HADs aggregation to 2.2km spatial units...")
            hads_resamplers: tuple[HADsResampler, ...] = (
                config.hads_manager.execute_resample_configs(
                    multiprocess=multiprocess, cpus=cpus
                )
            )
            print(hads_resamplers[:print_range_length])
        if skip_cropping or (not crop_cpm and not crop_hads):
            print("Skipping cropping.")
        else:
            if skip_cpm_standard_calendar_projection and not crop_cpm:
                print("Skipping cropping CPM Standard Calendar projections.")
            else:
                print(f"Cropping CPMs to regions {config.regions}: ...")
                cropped_cpm_resamplers: tuple[CPMResampler, ...] = (
                    config.cpm_manager.execute_crop_resamples(
                        multiprocess=multiprocess, cpus=cpus
                    )
                )
                print(cropped_cpm_resamplers[:print_range_length])
            if skip_cpm_standard_calendar_projection and not crop_hads:
                print("Skipping cropping HADS 2.2km projections.")
            else:
                print(
                    f"Cropping HADS 2.2km projections to regions {config.regions}: ..."
                )
                cropped_hads_resamplers: tuple[CPMResampler, ...] = (
                    config.hands_manager.execute_crop_resamples(
                        multiprocess=multiprocess, cpus=cpus
                    )
                )
                print(cropped_hads_resamplers[:print_range_length])
    else:
        print("No steps run. Add '--execute' to run steps.")

    # config.cpm.resample_multiprocessing()
