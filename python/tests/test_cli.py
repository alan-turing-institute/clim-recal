import pytest
from dotenv import load_dotenv
from typer.testing import CliRunner

from clim_recal.ceda_ftp_download import check_env_auth
from clim_recal.cli import ceda, cli, crop

load_dotenv()

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


@pytest.mark.download
@pytest.mark.skipif(
    not check_env_auth(), reason="CEDA user_name and password env required"
)
def test_ceda_cli(ceda_user_name: str, ceda_password: str) -> None:
    """Test `crop` function outside `cli`."""
    ceda(user_name=ceda_user_name, password=ceda_password)
