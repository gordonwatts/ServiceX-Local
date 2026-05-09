import getpass
from unittest.mock import MagicMock

import pytest
from servicex_local import install_sx_local, Platform
from servicex_local import DockerScienceImage, SingularityScienceImage, WSL2ScienceImage


@pytest.mark.parametrize(
    "image, platform, expected_class",
    [
        (
            "sslhep/servicex_func_adl_xaod_transformer:25.2.41",
            Platform.docker,
            DockerScienceImage,
        ),
        (
            "docker://sslhep/servicex_func_adl_xaod_transformer:25.2.41",
            Platform.singularity,
            SingularityScienceImage,
        ),
        ("servicex_func_adl_xaod_transformer:25.2.41", Platform.wsl2, WSL2ScienceImage),
    ],
)
def test_install_sx_local(monkeypatch, image, platform, expected_class):
    adaptor = install_sx_local(image, platform)
    assert isinstance(adaptor.science_runner, expected_class)


def test_install_sx_local_errors():
    with pytest.raises(ValueError, match="Unknown platform"):
        install_sx_local("some_image", platform="invalid_platform")


def test_install_sx_local_uses_yaml_cache_path(monkeypatch, tmp_path):
    "install_sx_local takes cache_path from servicex.yaml when one is found."
    yaml_cache = tmp_path / "from_yaml"
    yaml_cache.mkdir()

    fake_cfg = MagicMock()
    fake_cfg.cache_path = str(yaml_cache)
    monkeypatch.setattr(
        "servicex.configuration.Configuration.read",
        classmethod(lambda cls: fake_cfg),
    )

    adaptor = install_sx_local(
        "sslhep/servicex_func_adl_xaod_transformer:25.2.41", Platform.docker
    )

    assert adaptor.cache_dir == yaml_cache.resolve() / f"servicex_{getpass.getuser()}"


def test_install_sx_local_no_yaml_fallback(monkeypatch):
    """install_sx_local falls back to a usable cache dir when no
    servicex.yaml is found.

    Regression: the fallback used to assign cache_dir from inside a
    ``with tempfile.TemporaryDirectory()`` block, leaving the path dangling
    after the block exited.
    """

    def raise_nameerror(cls):
        raise NameError("Can't find .servicex or servicex.yaml config file")

    monkeypatch.setattr(
        "servicex.configuration.Configuration.read",
        classmethod(raise_nameerror),
    )

    adaptor = install_sx_local(
        "sslhep/servicex_func_adl_xaod_transformer:25.2.41", Platform.docker
    )

    # The fallback must produce a cache_dir whose parent exists on disk so
    # the adaptor can actually write into it.
    assert adaptor.cache_dir.parent.exists(), (
        f"Fallback cache_dir parent does not exist: {adaptor.cache_dir.parent}. "
        "The TemporaryDirectory was likely cleaned up before being used."
    )
