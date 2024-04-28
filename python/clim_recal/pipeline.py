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
- [ ] Ensure HADs still works
- [x] Add function for UKCP
- [x] Check `convert_xr_calendar` `doctest` examples

```python
from clim_recal.resample import CPMResampleManager

# Below assumes running with data mounted to `/mnt/vmfileshare/` following the `linux` config
for measure in ("tasmax", "tasmin"):
    # Assuming indexing doesn't go above 10
    for i in range(5, 9):
        # Create instance specifying paths
        cpm_resampler= CPMResampleManager(
            input_path=f'/mnt/vmfileshare/ClimateData/Raw/UKCP2.2/{measure}/0{i}/latest',
            standard_calendar_path=f"cpm-standard-calendar/{measure}/0{i}"
        )
        # Indicate running the standard calandar projection over all elements in `cpm_resampler`
        cpm_resampler_tasmin.to_standard_calendar(slice(0, len(cpm_resampler)))
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
    CityOptions,
    ClimRecalConfig,
    ClimRecalRunResultsType,
    MethodOptions,
    RunOptions,
    VariableOptions,
)

REPROJECTION_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_one.sh")
REPROJECTION_WRAPPER_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_all.sh")


def main(
    execute: bool = False,
    variables: Sequence[VariableOptions] = (VariableOptions.default(),),
    cities: Sequence[CityOptions] | None = (CityOptions.default(),),
    runs: Sequence[RunOptions] = (RunOptions.default(),),
    methods: Sequence[MethodOptions] = (MethodOptions.default(),),
    output_path: PathLike = DEFAULT_OUTPUT_PATH,
    cpus: int | None = None,
    **kwargs,
) -> ClimRecalRunResultsType:
    """Run all elements of the pipeline.

    Parameters
    ----------
    variables
        Variables to include in the model, eg. `tasmax`, `tasmin`.
    runs
        Which model runs to include, eg. "01", "08", "11".
    cities
        Which cities to crop data to. Future plans facilitate
        skipping to run for entire UK.
    methods
        Which debiasing methods to apply.
    output_path
        `Path` to save intermediate and final results to.
    cpus
        Number of cpus to use when parallelising.
    **kwargs
        Additional parameters to pass to a `ClimRecalConfig`.

    Notes
    -----
    The default parameters here are meant to reflect the entire
    workflow process to ease reproducibility.


    Examples
    --------
    >>> main(variables=("rainfall", "tasmin"),
    ...      output_path=resample_test_output_path)
    clim-recal pipeline configurations:
    <ClimRecalConfig(variables_count=2, runs_count=1,
                     cities_count=1, methods_count=1,
                     cpm_folders_count=2, hads_folders_count=2,
                     cpus=...)>
    <CPMResamplerManager(variables_count=2, runs_count=1,
                         input_files_count=2)>
    <HADsResamplerManager(variables_count=2, input_paths_count=2)>
    """
    config: ClimRecalConfig = ClimRecalConfig(
        output_path=output_path,
        variables=variables,
        cities=cities,
        methods=methods,
        runs=runs,
        cpus=cpus,
        **kwargs,
    )
    print("clim-recal pipeline configurations:")
    print(config)
    print(config.cpm_manger)
    print(config.hads_manger)
    if execute:
        print("Running CPM Manager process...")
        config.cpm_manger.run_resample_configs()
    # config.cpm.resample_multiprocessing()
