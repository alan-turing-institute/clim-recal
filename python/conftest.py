import sys

import pytest


@pytest.fixture()
def is_platform_darwin() -> bool:
    """Check if `sys.platform` is `Darwin` (macOS)."""
    return sys.platform.startswith("darwin")


@pytest.fixture(autouse=True)
def doctest_auto_fixtures(
    doctest_namespace: dict, is_platform_darwin: bool
) -> None:
    """Elements to add to default `doctest` namespace."""
    doctest_namespace["is_platform_darwin"] = is_platform_darwin
    doctest_namespace["pprint"] = pprint
    doctest_namespace["pytest"] = pytest
