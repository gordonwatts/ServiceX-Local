import os

import pytest


def pytest_addoption(parser):
    parser.addoption("--wsl2", action="store_true", help="run WSL2 tests")
    parser.addoption("--docker", action="store_true", help="run Docker tests")
    parser.addoption("--singularity", action="store_true", help="run Singularity tests")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "grid: test fetches files from grid storage and needs an x509 cert "
        "(auto-skipped when CI=true)",
    )


def pytest_collection_modifyitems(config, items):
    if os.environ.get("CI", "").lower() != "true":
        return
    skip_grid = pytest.mark.skip(
        reason="grid tests need an x509 cert; auto-skipped in CI"
    )
    for item in items:
        if "grid" in item.keywords:
            item.add_marker(skip_grid)
