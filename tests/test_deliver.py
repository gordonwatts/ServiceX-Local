import getpass
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from servicex import General, Sample, ServiceXSpec, dataset
from servicex.models import (
    ResultDestination,
    ResultFormat,
    Status,
    TransformRequest,
    TransformStatus,
)

from servicex_local import local_deliver
from servicex_local.configurations import Config


@pytest.fixture(autouse=True)
def restore_cache():
    """
    Fixture to restore the cache database to its original form after each test.
    """
    cache_dir: Path = (
        Path(tempfile.gettempdir()) / f"servicex_{getpass.getuser()}"
    )  # noqa: E501
    cache_file = cache_dir / "cache.json"
    original_cache = None

    if cache_file.exists():
        with cache_file.open("r") as f:
            original_cache = f.read()
        cache_file.unlink()

    yield

    if original_cache is not None:
        with cache_file.open("w") as f:
            f.write(original_cache)
    elif cache_file.exists():
        cache_file.unlink()


def _make_adaptor(cache_dir: Path):
    """Build a minimal in-memory adaptor whose cache_dir is configurable.

    submit_transform writes a fake output file to the location
    MinioLocalAdaptor reads from, so the full deliver flow works end-to-end.
    """

    class my_adaptor:
        def __init__(self):
            self._request_id = None
            self._submit_called = 0
            self.cache_dir = cache_dir

        @property
        def submit_called(self):
            return self._submit_called

        async def submit_transform(self, tq: TransformRequest) -> str:
            self._request_id = str(uuid.uuid4())

            assert tq.selection == "query1"

            file_path = (
                Path(tempfile.gettempdir())
                / f"servicex_{getpass.getuser()}"
                / self._request_id
                / "file1.root"
            )
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()

            self._submit_called += 1

            return self._request_id

        async def get_transform_status(self, request_id: str):
            assert request_id == self._request_id
            return TransformStatus(
                **{
                    "did": "file1",
                    "did_id": 0,
                    "selection": "q",
                    "request_id": request_id,
                    "status": Status.complete,
                    "tree-name": "my-tree",
                    "image": "doit",
                    "result-destination": ResultDestination.object_store,
                    "result-format": ResultFormat.root_ttree,
                    "files-completed": 1,
                    "files-failed": 0,
                    "files-remaining": 0,
                    "files": 1,
                    "app-version": "this",
                    "generated-code-cm": "this",
                    "submit-time": datetime.now(),
                }
            )

    return my_adaptor()


@pytest.fixture
def fake_install(tmp_path):
    """Patch install_sx_local so local_deliver gets a stub adaptor.

    Yields (adaptor, captured) where ``captured`` records the args
    install_sx_local was invoked with.
    """
    adaptor = _make_adaptor(tmp_path)
    captured: dict = {}

    def fake_install_sx_local(image, platform):
        captured["image"] = image
        captured["platform"] = platform
        return adaptor

    with patch(
        "servicex_local.deliver.install_sx_local", fake_install_sx_local
    ):
        yield adaptor, captured


def _spec():
    return ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="MySample",
                Dataset=dataset.FileList("test.root"),
                Query="query1",
            )
        ],
    )


def test_local_deliver_docker_image_string(fake_install):
    "docker platform passes a bare image:version to install_sx_local."
    _, captured = fake_install
    config = Config(version="25.2.41", platform="docker")

    r = local_deliver(_spec(), config, display_progress=False)

    assert r is not None
    assert "MySample" in r
    assert (
        captured["image"]
        == "sslhep/servicex_func_adl_xaod_transformer:25.2.41"
    )


def test_local_deliver_singularity_image_string(fake_install):
    "singularity platform prepends docker:// to the image string."
    _, captured = fake_install
    config = Config(version="25.2.41", platform="singularity")

    local_deliver(_spec(), config, display_progress=False)

    assert (
        captured["image"]
        == "docker://sslhep/servicex_func_adl_xaod_transformer:25.2.41"
    )


def test_local_deliver_wsl2_image_string(fake_install):
    "wsl2 platform uses the bare image:version string."
    _, captured = fake_install
    config = Config(version="25.2.41", platform="wsl2")

    local_deliver(_spec(), config, display_progress=False)

    assert (
        captured["image"]
        == "sslhep/servicex_func_adl_xaod_transformer:25.2.41"
    )


def test_local_deliver_awk_false_returns_dict(fake_install):
    "awk=False returns the deliver dict keyed by sample name."
    config = Config(version="25.2.41", awk=False)

    r = local_deliver(_spec(), config, display_progress=False)

    assert isinstance(r, dict)
    assert "MySample" in r
    assert len(r["MySample"]) == 1


def test_local_deliver_awk_true_returns_awk_for_mysample(fake_install):
    "awk=True returns the awkward-converted result for MySample."
    config = Config(version="25.2.41", awk=True)

    fake_awk = {"MySample": "awk_array_obj"}
    with patch(
        "servicex_local.deliver.to_awk", return_value=fake_awk
    ) as mock_to_awk:
        r = local_deliver(_spec(), config, display_progress=False)

    assert r == "awk_array_obj"
    assert mock_to_awk.call_count == 1

def _basic_spec() -> ServiceXSpec:
    return ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="test_me",
                Dataset=dataset.FileList("test.root"),
                Query="query1",
            )
        ],
    )


def test_deliver_warns_for_ignored_upstream_kwarg(simple_adaptor, caplog):
    "Passing an upstream-only kwarg logs a warning naming it."
    with caplog.at_level(logging.WARNING, logger="servicex_local.deliver"):
        deliver(_basic_spec(), adaptor=simple_adaptor, progress_bar="compact")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "ignored in servicex-local" in msg
    assert "progress_bar" in msg


def test_deliver_warns_for_multiple_ignored_kwargs(simple_adaptor, caplog):
    "All ignored upstream kwargs are listed in a single warning."
    with caplog.at_level(logging.WARNING, logger="servicex_local.deliver"):
        deliver(
            _basic_spec(),
            adaptor=simple_adaptor,
            progress_bar="compact",
            concurrency=4,
            servicex_name="prod",
        )

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    for name in ("progress_bar", "concurrency", "servicex_name"):
        assert name in msg


def test_deliver_no_warning_when_no_extra_kwargs(simple_adaptor, caplog):
    "No warning when only supported arguments are passed."
    with caplog.at_level(logging.WARNING, logger="servicex_local.deliver"):
        deliver(_basic_spec(), adaptor=simple_adaptor, ignore_local_cache=True)

    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_local_deliver_ignore_cache_true_resubmits(fake_install):
    "ignore_cache=True forces a re-submit on the second call."
    adaptor, _captured = fake_install
    config = Config(version="25.2.41", ignore_cache=True)

    local_deliver(_spec(), config, display_progress=False)
    local_deliver(_spec(), config, display_progress=False)

    assert adaptor.submit_called == 2


def test_local_deliver_ignore_cache_false_uses_cache(fake_install):
    "ignore_cache=False reuses the cached transform on the second call."
    adaptor, _captured = fake_install
    config = Config(version="25.2.41", ignore_cache=False)

    local_deliver(_spec(), config, display_progress=False)
    local_deliver(_spec(), config, display_progress=False)

    assert adaptor.submit_called == 1


@pytest.fixture(autouse=True)
def restore_root_logger():
    "Snapshot and restore the root logger's level + handlers around every test."
    root = logging.getLogger()
    saved_level = root.level
    saved_handlers = root.handlers[:]
    try:
        yield root
    finally:
        root.handlers[:] = saved_handlers
        root.setLevel(saved_level)


@pytest.mark.parametrize("level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
def test_config_logging_level_accepts_valid_names(level):
    "Config accepts every name the logging framework recognises."
    config = Config(version="25.2.41", logging_level=level)
    assert config.logging_level == level


def test_config_logging_level_rejects_invalid_string():
    "Config rejects strings that aren't logging level names."
    with pytest.raises(ValueError, match="logging_level"):
        Config(version="25.2.41", logging_level="LOUD")


def test_local_deliver_applies_logging_level_over_existing_handlers(
    fake_install, restore_root_logger
):
    "local_deliver overrides a pre-configured root logger (force=True)."
    root = restore_root_logger
    root.handlers[:] = [logging.NullHandler()]
    root.setLevel(logging.WARNING)

    config = Config(version="25.2.41", logging_level="DEBUG")
    local_deliver(_spec(), config, display_progress=False)

    assert root.level == logging.DEBUG


def test_config_logging_level_default_is_warning():
    "Default logging_level is WARNING (a recognised level name)."
    assert Config(version="25.2.41").logging_level == "WARNING"
