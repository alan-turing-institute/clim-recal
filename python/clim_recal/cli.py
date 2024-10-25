from datetime import datetime
from logging import getLogger
from os import cpu_count
from pathlib import Path
from typing import Annotated, Final, Optional

import typer

from .config import (
    DEFAULT_CPUS,
    DEFAULT_OUTPUT_PATH,
    ClimRecalRunResultsType,
    MethodOptions,
    RegionOptions,
    RunOptions,
    VariableOptions,
)
from .pipeline import CURRENT_CONVERTED_RESULTS_PATH, main, run_convert, run_crop
from .utils.core import CLI_DATE_FORMATS, date_str_infer_end
from .utils.data import (
    CPM_END_DATETIME,
    CPM_NAME,
    CPM_RAW_FOLDER,
    CPM_START_DATETIME,
    HADS_END_DATETIME,
    HADS_NAME,
    HADS_RAW_FOLDER,
    HADS_START_DATETIME,
    RAW_DATA_MOUNT_PATH,
)

logger = getLogger(__name__)

cli = typer.Typer()

CPU_COUNT: Final[int | None] = cpu_count()
MAX_CPUS: Final[int | None] = CPU_COUNT if CPU_COUNT else DEFAULT_CPUS

cli.pretty_exceptions_show_locals = False


def set_cli_debug() -> None:
    """Set into debug configuration mode."""
    cli.pretty_exceptions_show_locals = True


@cli.command()
def pipeline(
    input_path: Annotated[
        Optional[Path],
        typer.Option(
            "--input-path",
            file_okay=True,
            dir_okay=True,
            exists=True,
        ),
    ] = RAW_DATA_MOUNT_PATH,
    hads_path: Annotated[
        Optional[Path],
        typer.Option(
            "--hads-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(HADS_RAW_FOLDER),
    cpm_path: Annotated[
        Optional[Path],
        typer.Option(
            "--cpm-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(CPM_RAW_FOLDER),
    output_path: Annotated[
        Path,
        typer.Option(
            "--output-path", "-o", file_okay=False, dir_okay=True, writable=True
        ),
    ] = DEFAULT_OUTPUT_PATH,
    variable: Annotated[list[VariableOptions], typer.Option("--variable", "-v")] = [
        VariableOptions.default()
    ],
    region: Annotated[list[RegionOptions], typer.Option("--region", "-a")] = [
        RegionOptions.default()
    ],
    run: Annotated[list[RunOptions], typer.Option("--run", "-r")] = [
        RunOptions.default()
    ],
    method: Annotated[list[MethodOptions], typer.Option("--method", "-m")] = [
        MethodOptions.default()
    ],
    all_variables: Annotated[bool, typer.Option("--all-variables")] = False,
    all_regions: Annotated[bool, typer.Option("--all-regions")] = False,
    all_runs: Annotated[bool, typer.Option("--all-runs")] = False,
    default_runs: Annotated[bool, typer.Option("--default-runs")] = False,
    all_methods: Annotated[bool, typer.Option("--all-methods")] = False,
    cpm: Annotated[bool, typer.Option("--cpm/--not-cpm")] = True,
    hads: Annotated[bool, typer.Option("--hads/--not-hads")] = True,
    convert: Annotated[bool, typer.Option("--convert/--not-convert")] = True,
    crop: Annotated[bool, typer.Option("--crop/--not-crop")] = True,
    execute: Annotated[bool, typer.Option("--execute")] = False,
    start_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = None,
    end_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = None,
    hads_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = HADS_START_DATETIME,
    hads_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = HADS_END_DATETIME,
    cpm_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = CPM_START_DATETIME,
    cpm_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = CPM_END_DATETIME,
    convert_start_index: Annotated[
        int, typer.Option("--convert-start-index", min=0)
    ] = 0,
    convert_stop_index: Annotated[
        Optional[int], typer.Option("--convert-stop-index", min=1)
    ] = None,
    crop_start_index: Annotated[int, typer.Option("--crop-start-index", min=0)] = 0,
    crop_stop_index: Annotated[
        Optional[int], typer.Option("--crop-stop-index", min=1)
    ] = None,
    total: Annotated[int, typer.Option("--total-from-index", "-t", min=0)] = 0,
    calc_start_index: Annotated[int, typer.Option("--calc-start-index", min=0)] = 0,
    calc_stop_index: Annotated[
        Optional[int], typer.Option("--calc-stop-index", min=1)
    ] = None,
    cpus: Annotated[int, typer.Option("--cpus", min=1, max=MAX_CPUS)] = DEFAULT_CPUS,
    multiprocess: Annotated[bool, typer.Option("--use-multiprocessing")] = False,
    cpm_for_coord_alignment: Annotated[
        Optional[Path], typer.Option("--converted-cpm-for-hads")
    ] = None,
    debug_mode: Annotated[bool, typer.Option("--debug")] = False,
) -> ClimRecalRunResultsType | None:
    """Align and crop UK climate measures and projections and run debias methods."""
    if debug_mode:
        set_cli_debug()
    results: ClimRecalRunResultsType | None = main(
        input_path=input_path,
        hads_path=hads_path,
        cpm_path=cpm_path,
        variables=variable,
        runs=run,
        regions=region if crop else None,
        methods=method,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
        cpm=cpm,
        hads=hads,
        convert=convert,
        crop=crop,
        hads_start_date=start_date.date() if start_date else hads_start_date.date(),
        hads_end_date=end_date if end_date else hads_end_date,
        cpm_start_date=start_date.date() if start_date else cpm_start_date.date(),
        cpm_end_date=end_date if end_date else cpm_end_date,
        convert_start_index=convert_start_index,
        crop_start_index=crop_start_index,
        convert_stop_index=convert_stop_index,
        crop_stop_index=crop_stop_index,
        total=total,
        calc_start_index=calc_start_index,
        calc_stop_index=calc_stop_index,
        multiprocess=multiprocess,
        all_variables=all_variables,
        all_regions=all_regions,
        all_runs=all_runs,
        default_runs=default_runs,
        all_methods=all_methods,
        cpm_for_coord_alignment=cpm_for_coord_alignment,
        cli=cli,
        debug_mode=debug_mode,
    )
    # print(results)
    return results


@cli.command()
def convert(
    input_path: Annotated[
        Optional[Path],
        typer.Option(
            "--input-path",
            file_okay=True,
            dir_okay=True,
            exists=True,
        ),
    ] = RAW_DATA_MOUNT_PATH,
    hads_path: Annotated[
        Optional[Path],
        typer.Option(
            "--hads-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(HADS_RAW_FOLDER),
    cpm_path: Annotated[
        Optional[Path],
        typer.Option(
            "--cpm-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(CPM_RAW_FOLDER),
    output_path: Annotated[
        Path,
        typer.Option(
            "--output-path", "-o", file_okay=False, dir_okay=True, writable=True
        ),
    ] = DEFAULT_OUTPUT_PATH,
    variable: Annotated[list[VariableOptions], typer.Option("--variable", "-v")] = [
        VariableOptions.default()
    ],
    run: Annotated[list[RunOptions], typer.Option("--run", "-r")] = [
        RunOptions.default()
    ],
    all_variables: Annotated[bool, typer.Option("--all-variables")] = False,
    all_runs: Annotated[bool, typer.Option("--all-runs")] = False,
    default_runs: Annotated[bool, typer.Option("--default-runs")] = False,
    cpm: Annotated[bool, typer.Option("--cpm/--not-cpm")] = True,
    hads: Annotated[bool, typer.Option("--hads/--not-hads")] = True,
    execute: Annotated[bool, typer.Option("--execute")] = False,
    start_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = None,
    end_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = None,
    hads_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = HADS_START_DATETIME,
    hads_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = HADS_END_DATETIME,
    cpm_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = CPM_START_DATETIME,
    cpm_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = CPM_END_DATETIME,
    start_index: Annotated[int, typer.Option("--start-index", min=0)] = 0,
    stop_index: Annotated[Optional[int], typer.Option("--stop-index", min=1)] = None,
    total: Annotated[int, typer.Option("--total-from-index", "-t", min=0)] = 0,
    calc_start_index: Annotated[int, typer.Option("--calc-start-index", min=0)] = 0,
    calc_stop_index: Annotated[
        Optional[int], typer.Option("--calc-stop-index", min=1)
    ] = None,
    cpus: Annotated[int, typer.Option("--cpus", min=1, max=MAX_CPUS)] = DEFAULT_CPUS,
    multiprocess: Annotated[bool, typer.Option("--use-multiprocessing")] = False,
    cpm_for_coord_alignment: Annotated[
        Optional[Path], typer.Option("--converted-cpm-for-hads")
    ] = None,
    debug_mode: Annotated[bool, typer.Option("--debug")] = False,
) -> None:
    """Convert and align HadsUK climate measures and UKCPM projections."""
    if debug_mode:
        set_cli_debug()

    run_convert(
        config=None,
        input_path=input_path,
        hads_path=hads_path,
        cpm_path=cpm_path,
        variables=variable,
        runs=run,
        regions=None,
        methods=None,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
        cpm=cpm,
        hads=hads,
        crop=False,
        hads_start_date=start_date.date() if start_date else hads_start_date.date(),
        hads_end_date=end_date if end_date else hads_end_date,
        cpm_start_date=start_date.date() if start_date else cpm_start_date.date(),
        cpm_end_date=end_date if end_date else cpm_end_date,
        convert_start_index=start_index,
        convert_stop_index=stop_index,
        crop_start_index=start_index,  # keeping crop in case cli interactive
        crop_stop_index=stop_index,
        total=total,
        calc_start_index=calc_start_index,
        calc_stop_index=calc_stop_index,
        multiprocess=multiprocess,
        all_variables=all_variables,
        all_regions=False,
        all_runs=all_runs,
        default_runs=default_runs,
        all_methods=False,
        cpm_for_coord_alignment=cpm_for_coord_alignment,
        cli=cli,
        debug_mode=debug_mode,
    )


@cli.command()
def crop(
    input_path: Annotated[
        Optional[Path],
        typer.Option(
            "--input-path",
            file_okay=True,
            dir_okay=True,
            exists=True,
        ),
    ] = CURRENT_CONVERTED_RESULTS_PATH,
    hads_path: Annotated[
        Optional[Path],
        typer.Option(
            "--hads-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(HADS_NAME),
    cpm_path: Annotated[
        Optional[Path],
        typer.Option(
            "--cpm-path",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(CPM_NAME),
    output_path: Annotated[
        Path,
        typer.Option(
            "--output-path", "-o", file_okay=False, dir_okay=True, writable=True
        ),
    ] = DEFAULT_OUTPUT_PATH,
    variable: Annotated[list[VariableOptions], typer.Option("--variable", "-v")] = [
        VariableOptions.default()
    ],
    region: Annotated[list[RegionOptions], typer.Option("--region", "-a")] = [
        RegionOptions.default()
    ],
    run: Annotated[list[RunOptions], typer.Option("--run", "-r")] = [
        RunOptions.default()
    ],
    all_variables: Annotated[bool, typer.Option("--all-variables")] = False,
    all_regions: Annotated[bool, typer.Option("--all-regions")] = False,
    all_runs: Annotated[bool, typer.Option("--all-runs")] = False,
    default_runs: Annotated[bool, typer.Option("--default-runs")] = False,
    cpm: Annotated[bool, typer.Option("--cpm/--not-cpm")] = True,
    hads: Annotated[bool, typer.Option("--hads/--not-hads")] = True,
    execute: Annotated[bool, typer.Option("--execute")] = False,
    start_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = None,
    end_date: Annotated[
        Optional[datetime],
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = None,
    hads_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = HADS_START_DATETIME,
    hads_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = HADS_END_DATETIME,
    cpm_start_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
        ),
    ] = CPM_START_DATETIME,
    cpm_end_date: Annotated[
        datetime,
        typer.Option(
            formats=CLI_DATE_FORMATS,
            parser=date_str_infer_end,
        ),
    ] = CPM_END_DATETIME,
    start_index: Annotated[int, typer.Option("--start-index", min=0)] = 0,
    stop_index: Annotated[Optional[int], typer.Option("--stop-index", min=1)] = None,
    total: Annotated[int, typer.Option("--total-from-index", "-t", min=0)] = 0,
    calc_start_index: Annotated[int, typer.Option("--calc-start-index", min=0)] = 0,
    calc_stop_index: Annotated[
        Optional[int], typer.Option("--calc-stop-index", min=1)
    ] = None,
    cpus: Annotated[int, typer.Option("--cpus", min=1, max=MAX_CPUS)] = DEFAULT_CPUS,
    multiprocess: Annotated[bool, typer.Option("--use-multiprocessing")] = False,
    debug_mode: Annotated[bool, typer.Option("--debug")] = False,
) -> None:
    """Crop converted HADs and CPM data by region."""
    if debug_mode:
        set_cli_debug()
    run_crop(
        config=None,
        input_path=input_path,
        hads_path=hads_path,
        cpm_path=cpm_path,
        variables=variable,
        runs=run,
        regions=region,
        methods=None,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
        cpm=cpm,
        hads=hads,
        convert=False,
        crop=True,
        hads_start_date=start_date.date() if start_date else hads_start_date.date(),
        hads_end_date=end_date if end_date else hads_end_date,
        cpm_start_date=start_date.date() if start_date else cpm_start_date.date(),
        cpm_end_date=end_date if end_date else cpm_end_date,
        convert_start_index=start_index,  # Keeping convert if cli interactive
        convert_stop_index=stop_index,
        crop_start_index=start_index,
        crop_stop_index=stop_index,
        total=total,
        calc_start_index=calc_start_index,
        calc_stop_index=calc_stop_index,
        multiprocess=multiprocess,
        all_variables=all_variables,
        all_regions=all_regions,
        all_runs=all_runs,
        default_runs=default_runs,
        cli=cli,
        debug_mode=debug_mode,
    )
