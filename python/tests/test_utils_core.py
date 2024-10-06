import pytest

from clim_recal.convert import CPMConvert
from clim_recal.utils.core import multiprocess_execute


@pytest.mark.parametrize("progress_bar", (False, True))
@pytest.mark.mount
def test_multiprocess_execute(progress_bar) -> None:
    """Test multiprocessing with CPMConvert.

    Notes
    -----
    This requires a full remote mount of raw data and currently
    failes if using local cache files.
    """
    cpm_converter: CPMConvert = CPMConvert(stop_index=3)
    exists_tuple: tuple[bool, bool, bool] = multiprocess_execute(
        cpm_converter, method_name="exists", progress_bar=progress_bar
    )
    assert exists_tuple == [True, True, True]
