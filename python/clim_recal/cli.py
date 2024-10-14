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
from .convert import RAW_CPM_PATH, RAW_HADS_PATH
from .pipeline import main

clim_recal = typer.Typer()

CPU_COUNT: Final[int | None] = cpu_count()
MAX_CPUS: Final[int | None] = CPU_COUNT if CPU_COUNT else DEFAULT_CPUS


@clim_recal.command()
def pipeline(
    hads_input_path: Annotated[
        Optional[Path],
        typer.Option(
            "--hads-input-path",
            "-d",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(RAW_HADS_PATH),
    cpm_input_path: Annotated[
        Optional[Path],
        typer.Option(
            "--cpm-input-path",
            "-o",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(RAW_CPM_PATH),
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
) -> ClimRecalRunResultsType | None:
    """Align and crop UK climate measures and projection and run debias methods."""
    results: ClimRecalRunResultsType | None = main(
        hads_input_path=hads_input_path,
        cpm_input_path=cpm_input_path,
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
    )
    # print(results)
    return results
