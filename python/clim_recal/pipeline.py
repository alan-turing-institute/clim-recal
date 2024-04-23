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

# Resample HadUK-Grid

Previous approach:

- `resampling_hads.py`

New approach:

- `resampling.py`
- check `x_grid` and `y_grid` interpolation
```
# the dataset to be resample must have dimensions named
# projection_x_coordinate and projection_y_coordinate .
resampled = data_360[[variable]].interp(
    projection_x_coordinate=x_grid,
    projection_y_coordinate=y_grid,
    method="linear",
)
```
- [ ] Refactor to facilitate testing
- [ ] Ensure HADs still works
- [ ] Add function for UKCP
- [ ] Check `convert_xr_calendar` `doctest` examples

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
from pathlib import Path
from typing import Final, Sequence

from rich import print

from .config import (
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
    variables: Sequence[VariableOptions] = (VariableOptions.default(),),
    cities: Sequence[CityOptions] | None = (CityOptions.default(),),
    runs: Sequence[RunOptions] = (RunOptions.default(),),
    methods: Sequence[MethodOptions] = (MethodOptions.default(),),
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
    **kwargs
        Additional parameters to pass to a `ClimRecalConfig`.
    """
    config: ClimRecalConfig = ClimRecalConfig(
        variables=variables, cities=cities, methods=methods, runs=runs, **kwargs
    )
    print(config)
    # config.cpm.resample_multiprocessing()
