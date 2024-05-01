from os import cpu_count
from pathlib import Path
from typing import Annotated, Final

import typer

from .config import (
    DEFAULT_CPUS,
    DEFAULT_OUTPUT_PATH,
    CityOptions,
    ClimRecalRunResultsType,
    MethodOptions,
    RunOptions,
    VariableOptions,
)
from .pipeline import main

clim_recal = typer.Typer()

CPU_COUNT: Final[int | None] = cpu_count()
MAX_CPUS: Final[int | None] = CPU_COUNT if CPU_COUNT else DEFAULT_CPUS


@clim_recal.command()
def pipeline(
    output_path: Annotated[
        Path,
        typer.Option(
            "--output-path", "-o", file_okay=False, dir_okay=True, writable=True
        ),
    ] = DEFAULT_OUTPUT_PATH,
    variable: Annotated[list[VariableOptions], typer.Option("--variable", "-v")] = [
        VariableOptions.default()
    ],
    city: Annotated[list[CityOptions], typer.Option("--city", "-c")] = [
        CityOptions.default()
    ],
    run: Annotated[list[RunOptions], typer.Option("--run", "-r")] = [
        RunOptions.default()
    ],
    method: Annotated[list[MethodOptions], typer.Option("--method", "-m")] = [
        MethodOptions.default()
    ],
    all_variables: Annotated[bool, typer.Option("--all-variables")] = False,
    all_cities: Annotated[bool, typer.Option("--all-cities")] = False,
    all_runs: Annotated[bool, typer.Option("--all-runs")] = False,
    default_runs: Annotated[bool, typer.Option("--default-runs")] = False,
    all_methods: Annotated[bool, typer.Option("--all-methods")] = False,
    skip_cpm_projection: Annotated[bool, typer.Option("--skip-cpm-projection")] = False,
    skip_hads_projection: Annotated[
        bool, typer.Option("--skip-hads-projection")
    ] = False,
    execute: Annotated[bool, typer.Option("--execute")] = False,
    start_index: Annotated[int, typer.Option("--start-index", "-s", min=0)] = 0,
    total: Annotated[int, typer.Option("--total-from-index", "-t", min=0)] = 0,
    cpus: Annotated[
        int, typer.Option("--cpus", "-p", min=1, max=MAX_CPUS)
    ] = DEFAULT_CPUS,
) -> ClimRecalRunResultsType:
    """Run all or portions of UK climate projection debiasing methods."""
    stop_index: int | None = None if total == 0 else start_index + total
    variables: tuple[VariableOptions | str, ...] = (
        VariableOptions.all() if all_variables else tuple(variable)
    )
    cities: tuple[CityOptions | str, ...] = (
        CityOptions.all() if all_cities else tuple(city)
    )
    methods: tuple[MethodOptions | str, ...] = (
        MethodOptions.all() if all_methods else tuple(method)
    )
    runs: tuple[RunOptions | str, ...]

    print("vars:", variables)

    if all_runs:
        runs = RunOptions.all()
    elif default_runs:
        runs = RunOptions.preferred()
    else:
        runs = tuple(run)

    print("vars after conditional:", variables)

    results: ClimRecalRunResultsType = main(
        variables=variables,
        runs=runs,
        cities=cities,
        methods=methods,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
        skip_cpm_standard_calendar_projection=skip_cpm_projection,
        skip_hads_spatial_2k_projection=skip_hads_projection,
        start_index=start_index,
        stop_index=stop_index,
    )
    # print(results)
    return results
