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

- `convert.py`
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
                 folders_count=12, hads_folders_count=3, convert_start_index=0,
                 convert_stop_index=None, cpus=2)>
<CPMConvertManager(variables_count=3, runs_count=4, input_paths_count=12)>
<HADsConvertManager(variables_count=3, input_paths_count=3)>
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
                 folders_count=12, hads_folders_count=3, convert_start_index=0,
                 convert_stop_index=None, cpus=2)>
<CPMConvertManager(variables_count=3, runs_count=4, input_paths_count=12)>
<HADsConvertManager(variables_count=3, input_paths_count=3)>
Running CPM Standard Calendar projection...
<CPMConvert(count=100, max_count=100,
              input_path='/mnt/vmfileshare/ClimateData/Raw/UKCP2.2/tasmax/05/latest',
              output_path='/mnt/vmfileshare/ClimateData/CPM-365/test-run-3-may/convert/
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

from datetime import date
from os import PathLike, getenv
from pathlib import Path
from typing import Final, Sequence

from dotenv import load_dotenv
from xarray.core.types import T_Dataset

from clim_recal.utils.data import HADS_AND_CPM, HADS_NAME, get_clim_types

from .config import (
    DEFAULT_OUTPUT_PATH,
    ClimDataTypeTuple,
    ClimRecalConfig,
    ClimRecalRunResultsType,
    ClimRecanConfigError,
    MethodOptions,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .convert import CPMConvert, HADsConvert
from .crop import CPMRegionCropper, HADsRegionCropper
from .utils.core import console
from .utils.data import (
    CLIMATE_DATA_MOUNT_PATH,
    CPM_END_DATE,
    CPM_NAME,
    CPM_RAW_FOLDER,
    CPM_START_DATE,
    HADS_END_DATE,
    HADS_NAME,
    HADS_RAW_FOLDER,
    HADS_START_DATE,
    RAW_DATA_MOUNT_PATH,
)

load_dotenv()

CURRENT_MOUNT_RESULTS_PATH: Final[Path] = (
    CLIMATE_DATA_MOUNT_PATH / "nearest_2024-10-14/"
)
CURRENT_CONVERTED_RESULTS_PATH: Final[Path] = CURRENT_MOUNT_RESULTS_PATH / "resample"
EXECUTE_INFO_TEXT: Final[str] = "Add '--execute' to run."


def get_config(
    input_path: PathLike = RAW_DATA_MOUNT_PATH,
    hads_path: PathLike = HADS_RAW_FOLDER,
    cpm_path: PathLike = CPM_RAW_FOLDER,
    output_path: PathLike = DEFAULT_OUTPUT_PATH,
    cpm: bool = True,
    hads: bool = True,
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),),
    regions: Sequence[RegionOptions | str] | None = (RegionOptions.default(),),
    runs: Sequence[RunOptions | str] = (RunOptions.default(),),
    methods: Sequence[MethodOptions | str] = (MethodOptions.default(),),
    all_variables: bool = False,
    all_regions: bool = False,
    default_runs: bool = False,
    all_runs: bool = False,
    all_methods: bool = False,
    cpus: int | None = None,
    multiprocess: bool = False,
    ceda_download: bool = False,
    convert: bool = True,
    crop: bool = True,
    hads_start_date: date = HADS_START_DATE,
    hads_end_date: date = HADS_END_DATE,
    cpm_start_date: date = CPM_START_DATE,
    cpm_end_date: date = CPM_END_DATE,
    convert_start_index: int = 0,
    convert_stop_index: int | None = None,
    crop_start_index: int = 0,
    crop_stop_index: int | None = None,
    total: int | None = None,
    cpm_for_coord_alignment: PathLike | T_Dataset | None = None,
    **kwargs,
) -> ClimRecalConfig:
    clim_types: ClimDataTypeTuple = get_clim_types(hads=hads, cpm=cpm)
    if not clim_types:
        raise ClimRecanConfigError(
            f"At least one of '{HADS_NAME}' or '{CPM_NAME}' required"
        )
    if not "ceda_download_kwargs" in kwargs:
        kwargs["ceda_download_kwargs"] = {}
    for ceda_download_var in ("user_name", "password"):

        if ceda_download_var in kwargs:
            kwargs["ceda_download_kwargs"][ceda_download_var] = kwargs.pop(
                ceda_download_var
            ) or getenv(f"CEDA_ENV_{ceda_download_var}_KEY")
    if convert:
        if convert_stop_index and total:
            console.print(
                f"Both 'convert_stop_index': {convert_stop_index} and 'total': {total} provided, skipping 'total'."
            )
        elif total:
            convert_stop_index = (
                None if total == 0 or total == None else convert_start_index + total
            )
            console.print(
                f"'convert_stop_index': {convert_stop_index} set from 'total': {total} and 'convert_start_index': {convert_start_index}."
            )

    if crop:
        if crop_stop_index and total:
            console.print(
                f"Both 'crop_stop_index': {crop_stop_index} and 'total': {total} provided, skipping 'total'."
            )
        elif total:
            crop_stop_index = (
                None if total == 0 or total == None else crop_start_index + total
            )
            console.print(
                f"'crop_stop_index': {crop_stop_index} set from 'total': {total} and 'crop_start_index': {crop_start_index}."
            )
        regions = (
            RegionOptions.all() if all_regions else tuple(regions) if regions else None
        )
    else:
        regions = None

    variables = VariableOptions.all() if all_variables else tuple(variables)
    if methods:
        methods = MethodOptions.all() if all_methods else tuple(methods)
    if all_runs:
        runs = RunOptions.all()
    elif default_runs:
        runs = RunOptions.preferred()
    else:
        runs = tuple(runs)

    return ClimRecalConfig(
        input_path=input_path,
        hads_path=hads_path,
        cpm_path=cpm_path,
        output_path=output_path,
        clim_types=clim_types,
        ceda_download=ceda_download,
        convert=convert,
        crop=crop,
        variables=variables,
        regions=regions,
        methods=methods,
        runs=runs,
        cpus=cpus,
        multiprocess=multiprocess,
        hads_start_date=hads_start_date,
        hads_end_date=hads_end_date,
        cpm_start_date=cpm_start_date,
        cpm_end_date=cpm_end_date,
        convert_start_index=convert_start_index,
        convert_stop_index=convert_stop_index,
        crop_start_index=crop_start_index,
        crop_stop_index=crop_stop_index,
        cpm_for_coord_alignment=cpm_for_coord_alignment,
        **kwargs,
    )


def run_ceda_download(
    config: ClimRecalConfig | None,
    user_name: str | None = None,
    password: str | None = None,
    print_range_length: int | None = 5,
    execute: bool = False,
    pipeline: bool = False,
    **kwargs,
) -> None:
    """Download `convert` parts of passed `config`."""
    kwargs["user_name"] = user_name
    kwargs["password"] = password
    config = config or get_config(**kwargs)
    if execute and config.ceda_download:
        if config.include_cpm:
            console.print(f"Downloading {CPM_NAME} data...")
            downloads: tuple[CPMConvert, ...] = (
                config.cpm_ceda_download_manager.download(
                    # multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(downloads[:print_range_length])
        else:
            console.print(f"Skipping downloading {CPM_NAME} data.")
        if config.include_hads:
            console.print(f"Downloading {HADS_NAME} data...")
            hads_downloads: tuple[HADsConvert, ...] = (
                config.hads_ceda_download_manager.download(
                    # multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(hads_downloads[:print_range_length])
        else:
            console.print(f"Skipping downloading {HADS_NAME} data.")
    else:
        if pipeline:
            console.print(f"Skipping downloads.")
        else:
            console.print(EXECUTE_INFO_TEXT)


def run_convert(
    config: ClimRecalConfig | None,
    print_range_length: int | None = 5,
    execute: bool = False,
    pipeline: bool = False,
    **kwargs,
) -> None:
    """Run `convert` parts of passed `config`."""
    config = config or get_config(**kwargs)
    if config.include_cpm:
        console.print(config.cpm_convert_manager)
    if config.include_hads:
        console.print(config.hads_convert_manager)
    if execute and config.convert:
        if config.include_cpm:
            console.print(f"Running {CPM_NAME} conversion...")
            cpm_converters: tuple[CPMConvert, ...] = (
                config.cpm_convert_manager.execute_configs(
                    multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(cpm_converters[:print_range_length])
        else:
            console.print(f"Skipping {CPM_NAME} Strandard Calendar projection.")
        if config.include_hads:
            console.print(f"Running {HADS_NAME} conversion...")
            hads_converters: tuple[HADsConvert, ...] = (
                config.hads_convert_manager.execute_configs(
                    multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(hads_converters[:print_range_length])
        else:
            console.print(f"Skipping {HADS_NAME} aggregation to 2.2km spatial units.")
    else:
        if pipeline:
            console.print(f"Skipping conversion.")
        else:
            console.print(EXECUTE_INFO_TEXT)


def run_crop(
    config: ClimRecalConfig | None,
    input_path: PathLike = CURRENT_CONVERTED_RESULTS_PATH,
    hads_path: PathLike = Path(HADS_NAME),
    cpm_path: PathLike = Path(CPM_NAME),
    print_range_length: int | None = 5,
    execute: bool = False,
    pipeline: bool = False,
    **kwargs,
) -> None:
    """Run `crop` parts of passed `config`."""
    config = config or get_config(
        input_path=input_path, hads_path=hads_path, cpm_path=cpm_path, **kwargs
    )
    if config.regions:
        if config.include_cpm:
            console.print(config.cpm_crop_manager)
        if config.include_hads:
            console.print(config.hads_crop_manager)
    if execute and config.crop:
        if config.include_cpm:
            console.print(
                f"Cropping {CPM_NAME} conversions to regions {config.regions}: ..."
            )
            region_cropped_converts: tuple[CPMRegionCropper, ...] = (
                config.cpm_crop_manager.execute_configs(
                    multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(region_cropped_converts[:print_range_length])
        else:
            console.print(f"Skipping cropping {CPM_NAME} conversions.")
        if config.include_hads:
            console.print(
                f"Cropping {HADS_AND_CPM} conversions to regions {config.regions}: ..."
            )
            region_cropped_hads_converts: tuple[HADsRegionCropper, ...] = (
                config.hads_crop_manager.execute_configs(
                    multiprocess=config.multiprocess, cpus=config.cpus
                )
            )
            console.print(region_cropped_hads_converts[:print_range_length])

        else:
            console.print(f"Skipping cropping {HADS_NAME} conversions.")
    else:
        if pipeline:
            console.print("Skipping region cropping.")
        else:
            console.print(EXECUTE_INFO_TEXT)


def main(
    execute: bool = False,
    input_path: PathLike = RAW_DATA_MOUNT_PATH,
    hads_path: PathLike = HADS_RAW_FOLDER,
    cpm_path: PathLike = CPM_RAW_FOLDER,
    output_path: PathLike = DEFAULT_OUTPUT_PATH,
    hads: bool = True,
    cpm: bool = True,
    variables: Sequence[VariableOptions | str] = (VariableOptions.default(),),
    regions: Sequence[RegionOptions | str] | None = (RegionOptions.default(),),
    runs: Sequence[RunOptions | str] = (RunOptions.default(),),
    methods: Sequence[MethodOptions | str] | None = (MethodOptions.default(),),
    all_variables: bool = False,
    all_regions: bool = False,
    default_runs: bool = False,
    all_runs: bool = False,
    all_methods: bool = False,
    ceda_download: bool = False,
    convert: bool = True,
    crop: bool = True,
    cpus: int | None = None,
    multiprocess: bool = False,
    convert_start_index: int = 0,
    convert_stop_index: int | None = None,
    crop_start_index: int = 0,
    crop_stop_index: int | None = None,
    total: int | None = None,
    print_range_length: int | None = 5,
    cpm_for_coord_alignment: PathLike | T_Dataset | None = None,
    **kwargs,
) -> ClimRecalRunResultsType | None:
    """Run all elements of the pipeline.

    Parameters
    ----------
    cpm
        Whether to process `cpm` data.
    hads
        Whether to process `hads` data.
    variables
        Variables to include in the model, eg. `tasmax`, `tasmin`.
    runs
        Which model runs to include, eg. "01", "08", "11".
    regions
        Which regions to crop data to. Future plans facilitate
        skipping to run for entire UK.
    ceda_download
        Whether to download raw data from the Centre for
        Enviornmental Data Analysis (CEDA). If `False`, assumes
        that data is available in the `input_path`.
    methods
        Which debiasing methods to apply.
    output_path
        `Path` to save intermediate and final results to.
    cpus
        Number of cpus to use when multiprocessing.
    multiprocess
        Whether to use multiprocessing.
    convert_start_index
        Index to start all iterations from.
    convert_stop_index
        Number of files from `convert_start_index` to convert.
    crop_start_index
        Index to start all crop iterations from.
    crop_stop_index
        Number of files iterating from `crop_start_index` to crop.
    total
        Total number of records to iterate over. 0 and
        `None` indicate all values from `convert_start_index`.
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
    Defaulting to: '...'
    clim-recal pipeline configurations:
    <ClimRecalConfig(variables_count=2, runs_count=1,
                     regions_count=1, methods_count=1,
                     cpm_folders_count=2, hads_folders_count=2,
                     convert_start_index=0, convert_stop_index=None,
                     crop_start_index=0, crop_stop_index=None, cpus=...)>
    Skipping downloads.
    <CPMConvertManager(start_date=date(1980, 12, 1),
                       end_date=date(2080, 11, 30),
                       variables_count=2,
                       runs_count=1,
                       input_paths_count=2)>
    <HADsConvertManager(start_date=date(1980, 1, 1),
                        end_date=date(2021, 12, 31),
                        variables_count=2,
                        input_paths_count=2)>
    Skipping conversion.
    <CPMRegionCropManager(start_date=date(1980, 12, 1),
                          end_date=date(2080, 11, 30),
                          variables_count=2,
                          input_paths_count=2)>
    <HADsRegionCropManager(start_date=date(1980, 1, 1),
                           end_date=date(2021, 12, 31),
                           variables_count=2,
                           input_paths_count=2)>
    Skipping region cropping.
    Add '--execute' to run.
    """
    config: ClimRecalConfig = get_config(
        input_path=input_path,
        hads_path=hads_path,
        cpm_path=cpm_path,
        output_path=output_path,
        hads=hads,
        cpm=cpm,
        variables=variables,
        regions=regions,
        runs=runs,
        methods=methods,
        ceda_download=ceda_download,
        convert=convert,
        crop=crop,
        all_variables=all_variables,
        all_regions=all_regions,
        default_runs=default_runs,
        all_runs=all_runs,
        all_methods=all_methods,
        cpus=cpus,
        multiprocess=multiprocess,
        convert_start_index=convert_start_index,
        convert_stop_index=convert_stop_index,
        crop_start_index=crop_start_index,
        crop_stop_index=crop_stop_index,
        total=total,
        cpm_for_coord_alignment=cpm_for_coord_alignment,
        **kwargs,
    )
    console.print("clim-recal pipeline configurations:")
    console.print(config)
    run_ceda_download(
        config, execute=execute, pipeline=True, print_range_length=print_range_length
    )
    run_convert(
        config, execute=execute, pipeline=True, print_range_length=print_range_length
    )
    run_crop(
        config, execute=execute, pipeline=True, print_range_length=print_range_length
    )
    if not execute:
        console.print(EXECUTE_INFO_TEXT)
