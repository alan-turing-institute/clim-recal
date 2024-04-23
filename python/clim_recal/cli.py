import typer
from rich import print

from .config import (
    CityOptions,
    ClimRecalConfig,
    ClimRecalRunResultsType,
    MethodOptions,
    RunOptions,
    VariableOptions,
)
from .pipeline import main

clim_recal = typer.Typer()


@clim_recal.command()
def pipeline(
    variables: list[VariableOptions] = [VariableOptions.default()],
    cities: list[CityOptions] = [CityOptions.default()],
    runs: list[RunOptions] = [RunOptions.default()],
    methods: list[MethodOptions] = [MethodOptions.default()],
) -> ClimRecalRunResultsType:
    results: ClimRecalConfig = main(
        variables=variables, runs=runs, cities=cities, methods=methods
    )
    print(results)
