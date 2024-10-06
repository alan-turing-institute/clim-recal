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
- [x] Fix order of UKCP changes

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
                 cpm_folders_count=12, hads_folders_count=3, resample_start_index=0,
                 resample_stop_index=None, cpus=2)>
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
                 cpm_folders_count=12, hads_folders_count=3, resample_start_index=0,
                 resample_stop_index=None, cpus=2)>
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

from .config import (
    DEFAULT_OUTPUT_PATH,
    ClimRecalConfig,
    ClimRecalRunResultsType,
    MethodOptions,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .convert import RAW_CPM_PATH, RAW_HADS_PATH, CPMResampler, HADsResampler
from .crop import CPMRegionCropper, HADsRegionCropper
from .utils.core import console

REPROJECTION_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_one.sh")
REPROJECTION_WRAPPER_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_all.sh")


def main(
    execute: bool = False,
    hads_input_path: PathLike = RAW_HADS_PATH,
    cpm_input_path: PathLike = RAW_CPM_PATH,
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
    hads_projection: bool = False,
    cpm_projection: bool = False,
    crop_hads: bool = True,
    crop_cpm: bool = True,
    cpus: int | None = None,
    multiprocess: bool = False,
    resample_start_index: int = 0,
    resample_stop_index: int | None = None,
    crop_start_index: int = 0,
    crop_stop_index: int | None = None,
    total: int | None = None,
    print_range_length: int | None = 5,
    **kwargs,
) -> ClimRecalRunResultsType | None:
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
    resample_start_index
        Index to start all iterations from.
    resample_stop_index
        Number of files from `resample_start_index` to resample.
    crop_start_index
        Index to start all crop iterations from.
    crop_stop_index
        Number of files iterating from `crop_start_index` to crop.
    total
        Total number of records to iterate over. 0 and
        `None` indicate all values from `resample_start_index`.
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

    >>> if not is_data_mounted:
    ...     pytest.skip(mount_doctest_skip_message)
    >>> main(variables=("rainfall", "tasmin"),
    ...      output_path=test_runs_output_path,
    ... )
    'set_cpm_for_coord_alignment' for 'HADs' not speficied.
    Defaulting to 'self.cpm_input_path': '...'
    clim-recal pipeline configurations:
    <ClimRecalConfig(variables_count=2, runs_count=1,
                     regions_count=1, methods_count=1,
                     cpm_folders_count=2, hads_folders_count=2,
                     resample_start_index=0, resample_stop_index=None,
                     crop_start_index=0, crop_stop_index=None, cpus=...)>
    <CPMResamplerManager(variables_count=2, runs_count=1,
                         input_paths_count=2)>
    <HADsResamplerManager(variables_count=2, input_paths_count=2)>
    <CPMRegionCropManager(variables_count=2, input_paths_count=2)>
    <HADsRegionCropManager(variables_count=2, input_paths_count=2)>
    No steps run. Add '--execute' to run steps.
    """
    if resample_stop_index and total:
        console.print(
            f"Both 'resample_stop_index': {resample_stop_index} and 'total': {total} provided, skipping 'total'."
        )
    elif total:
        resample_stop_index = (
            None if total == 0 or total == None else resample_start_index + total
        )
        console.print(
            f"'resample_stop_index': {resample_stop_index} set from 'total': {total} and 'resample_start_index': {resample_start_index}."
        )
    if resample_stop_index and total:
        console.print(
            f"Both 'resample_stop_index': {resample_stop_index} and 'total': {total} provided, skipping 'total'."
        )
    elif total:
        resample_stop_index = (
            None if total == 0 or total == None else resample_start_index + total
        )
        console.print(
            f"'resample_stop_index': {resample_stop_index} set from 'total': {total} and 'resample_start_index': {resample_start_index}."
        )
    variables = VariableOptions.all() if all_variables else tuple(variables)
    regions = (
        RegionOptions.all() if all_regions else tuple(regions) if regions else None
    )
    methods = MethodOptions.all() if all_methods else tuple(methods)
    if all_runs:
        runs = RunOptions.all()
    elif default_runs:
        runs = RunOptions.preferred()
    else:
        runs = tuple(runs)

    config: ClimRecalConfig = ClimRecalConfig(
        cpm_input_path=cpm_input_path,
        hads_input_path=hads_input_path,
        output_path=output_path,
        variables=variables,
        regions=regions,
        methods=methods,
        runs=runs,
        cpus=cpus,
        multiprocess=multiprocess,
        resample_start_index=resample_start_index,
        resample_stop_index=resample_stop_index,
        crop_start_index=crop_start_index,
        crop_stop_index=crop_stop_index,
        **kwargs,
    )
    console.print("clim-recal pipeline configurations:")
    console.print(config)
    console.print(config.cpm_manager)
    console.print(config.hads_manager)
    console.print(config.cpm_crop_manager)
    console.print(config.hads_crop_manager)
    if execute:
        if not cpm_projection:
            console.print("Skipping CPM Strandard Calendar projection.")
        else:
            console.print("Running CPM Standard Calendar projection...")
            cpm_resamplers: tuple[CPMResampler, ...] = (
                config.cpm_manager.execute_configs(multiprocess=multiprocess, cpus=cpus)
            )
            console.print(cpm_resamplers[:print_range_length])
            # Leaving assert to remind ease for debugging in future
            # assert False
        if not hads_projection:
            console.print("Skipping HADs aggregation to 2.2km spatial units.")
        else:
            console.print("Running HADs aggregation to 2.2km spatial units...")
            hads_resamplers: tuple[HADsResampler, ...] = (
                config.hads_manager.execute_configs(
                    multiprocess=multiprocess, cpus=cpus
                )
            )
            console.print(hads_resamplers[:print_range_length])
        if not crop_hads and not crop_cpm:
            console.print("Skipping region cropping.")
        else:
            if not crop_cpm:
                console.print("Skipping cropping CPM Standard Calendar projections.")
            else:
                console.print(f"Cropping CPMs to regions {config.regions}: ...")
                region_cropped_cpm_resamples: tuple[CPMRegionCropper, ...] = (
                    config.cpm_crop_manager.execute_configs(
                        multiprocess=multiprocess, cpus=cpus
                    )
                )
                console.print(region_cropped_cpm_resamples[:print_range_length])
            if not crop_hads:
                console.print("Skipping cropping HADS 2.2km projections.")
            else:
                console.print(
                    f"Cropping HADS 2.2km projections to regions {config.regions}: ..."
                )
                region_cropped_hads_resamples: tuple[HADsRegionCropper, ...] = (
                    config.hads_crop_manager.execute_configs(
                        multiprocess=multiprocess, cpus=cpus
                    )
                )
                console.print(region_cropped_hads_resamples[:print_range_length])
    else:
        console.print("No steps run. Add '--execute' to run steps.")
