import pytest

from clim_recal.resample import CPMResampler
from clim_recal.utils.core import multiprocess_execute


@pytest.mark.parametrize("progress_bar", (False, True))
@pytest.mark.mount
def test_multiprocess_execute(progress_bar) -> None:
    """Test multiprocessing with CPMResampler."""
    cpm_resampler: CPMResampler = CPMResampler(stop_index=3)
    exists_tuple: tuple[bool, bool, bool] = multiprocess_execute(
        cpm_resampler, method_name="exists", progress_bar=progress_bar
    )
    assert exists_tuple == [True, True, True]
