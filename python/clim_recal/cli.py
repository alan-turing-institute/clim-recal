from os import cpu_count
from pathlib import Path
from typing import Annotated, Final

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
from .pipeline import main
from .resample import RAW_CPM_PATH, RAW_HADS_PATH

clim_recal = typer.Typer()

CPU_COUNT: Final[int | None] = cpu_count()
MAX_CPUS: Final[int | None] = CPU_COUNT if CPU_COUNT else DEFAULT_CPUS


@clim_recal.command()
def pipeline(
    hads_input_path: Annotated[
        Path,
        typer.Option(
            "--hads-input-path",
            "-d",
            file_okay=True,
            dir_okay=True,
        ),
    ] = Path(RAW_HADS_PATH),
    cpm_input_path: Annotated[
        Path,
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
    cpm_projection: Annotated[bool, typer.Option("--project-cpm/--skip-project-cpm")] = True,
    hads_projection: Annotated[bool, typer.Option("--project-hads/--skip-project-hads")] = True,
    crop_hads: Annotated[bool, typer.Option("--crop-hads/--skip-crop-hads")] = True,
    crop_cpm: Annotated[bool, typer.Option("--crop-cpm/--skip-crop-cpm")] = True,
    execute: Annotated[bool, typer.Option("--execute")] = False,
    start_index: Annotated[int, typer.Option("--start-index", "-s", min=0)] = 0,
    total: Annotated[int, typer.Option("--total-from-index", "-t", min=0)] = 0,
    cpus: Annotated[int, typer.Option("--cpus", min=1, max=MAX_CPUS)] = DEFAULT_CPUS,
    multiprocess: Annotated[bool, typer.Option("--use-multiprocessing")] = False,
) -> ClimRecalRunResultsType | None:
    """Crop and align UK climate projections and test debias methods."""
    results: ClimRecalRunResultsType | None = main(
        hads_input_path=hads_input_path,
        cpm_input_path=cpm_input_path,
        variables=variable,
        runs=run,
        regions=region,
        methods=method,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
        cpm_projection=cpm_projection,
        hads_projection=hads_projection,
        crop_hads=crop_hads,
        crop_cpm=crop_cpm,
        start_index=start_index,
        total=total,
        multiprocess=multiprocess,
        all_variables=all_variables,
        all_regions=all_regions,
        all_runs=all_runs,
        default_runs=default_runs,
        all_methods=all_methods,
    )
    # print(results)
    return results
