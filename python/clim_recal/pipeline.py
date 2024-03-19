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



"""
from os import PathLike
from pathlib import Path
from typing import Any, Final

# from . import ceda_ftp_download, data_loader, resample
from .utils import is_platform_darwin

REPROJECTION_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_one.sh")
REPROJECTION_WRAPPER_SHELL_SCRIPT: Final[Path] = Path("../bash/reproject_all.sh")

DEBIAN_MOUNT_PATH: Final[Path] = Path("/mnt/vmfileshare")
DARWIN_MOUNT_PATH: Final[Path] = Path("/Volumnes/vmfileshare")
CLIMATE_DATA_PATH: Final[Path] = Path("ClimateData")


def climate_data_mount_path(
    is_darwin: bool | None = None, full_path: bool = True
) -> Path:
    """Return likely climate data mount path based on operating system.

    Parameters
    ----------
    is_darwin
        Whether to use `CLIMATE_DATA_MOUNT_PATH_DARWIN` or
        call `is_platform_darwin` if None. fixture `is_platform_darwin`.

    Returns
    -------
    The `Path` climate data would likely be mounted to.
    """
    path: Path
    if is_darwin is None:
        is_darwin = is_platform_darwin()
    if is_darwin:
        path = DARWIN_MOUNT_PATH
    else:
        path = DEBIAN_MOUNT_PATH
    if full_path:
        return path / CLIMATE_DATA_PATH
    else:
        return path


def is_climate_data_mounted(mount_path: PathLike | None = None) -> bool:
    """Check if `CLIMATE_DATA_MOUNT_PATH` is mounted.

    Notes
    -----
    Consider refactoring to `climate_data_mount_path` returns `None`
    when conditions here return `False`.
    """
    if not mount_path:
        mount_path = climate_data_mount_path()
    assert isinstance(mount_path, Path)
    return mount_path.exists()


def main(**kwargs) -> dict[str, Any]:
    """Run all elements of the pipeline."""
    pass
