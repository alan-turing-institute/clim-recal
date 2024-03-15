from clim_recal.pipeline import (
    CLIMATE_DATA_PATH,
    DARWIN_MOUNT_PATH,
    DEBIAN_MOUNT_PATH,
    climate_data_mount_path,
)
from clim_recal.utils import is_platform_darwin


def test_climate_data_mount_path() -> None:
    """Test OS specifc mount path."""
    if is_platform_darwin():
        assert climate_data_mount_path() == DARWIN_MOUNT_PATH / CLIMATE_DATA_PATH
    else:
        assert climate_data_mount_path() == DEBIAN_MOUNT_PATH / CLIMATE_DATA_PATH
