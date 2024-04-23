from typer.testing import CliRunner

from clim_recal.cli import clim_recal

runner: CliRunner = CliRunner()


def test_pipeline_cli() -> None:
    """Test basic cli"""
    result = runner.invoke(clim_recal, ["--help"])
    assert result.exit_code == 0
    assert "[Glasgow|Manchester|Lon" in result.stdout
