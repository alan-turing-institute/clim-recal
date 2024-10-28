import pytest
from typer.testing import CliRunner

from clim_recal.cli import cli, crop

runner: CliRunner = CliRunner()


@pytest.mark.darwin
@pytest.mark.parametrize("command", ("pipeline", "convert", "crop"))
def test_cli_help(command: str) -> None:
    """Test cli help options."""
    result = runner.invoke(cli, [command, "--help"])
    assert result.exit_code == 0
    for text in ("tasmax|rain", "--execute"):
        assert text in result.stdout
        if command not in ("convert",):
            assert "Glasgow|Manc" in result.stdout


@pytest.mark.mount
def test_crop_cli(is_data_mounted: bool) -> None:
    """Test `crop` function outside `cli`."""
    if not is_data_mounted:
        with pytest.raises(ValueError):
            crop()
    else:
        crop()
