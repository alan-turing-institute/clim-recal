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
        VariableOptions.all()
    ],
    city: Annotated[list[CityOptions], typer.Option("--city", "-c")] = [
        CityOptions.all()
    ],
    run: Annotated[list[RunOptions], typer.Option("--run", "-r")] = [
        RunOptions.preferred()
    ],
    method: Annotated[list[MethodOptions], typer.Option("--method", "-m")] = [
        MethodOptions.default()
    ],
    execute: Annotated[bool, typer.Option()] = False,
    cpus: Annotated[
        int, typer.Option("--cpus", "-p", min=1, max=MAX_CPUS)
    ] = DEFAULT_CPUS,
) -> ClimRecalRunResultsType:
    results: ClimRecalRunResultsType = main(
        variables=variable,
        runs=run,
        cities=city,
        methods=method,
        output_path=output_path,
        cpus=cpus,
        execute=execute,
    )
    # print(results)
    return results
