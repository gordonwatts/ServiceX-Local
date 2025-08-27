import pytest
from servicex_local import find_dataset, install_sx_local, Platform
from servicex_local import DockerScienceImage, SingularityScienceImage, WSL2ScienceImage


@pytest.mark.parametrize(
    "input_path, prefer_local, expected_local, fs_file",
    [
        # Exact local file path
        ("/data/data.root", True, True, True),
        ("/data/data.root", False, True, True),
        # file:// URI
        ("file:///data/data.root", True, True, True),
        ("file:///data/data.root", False, True, True),
        # Plain URL
        ("https://test.com", True, True, False),
        ("https://test.com", False, False, False),
        # CERNBox remote file
        ("https://cernbox.cern.ch/files/spaces/test.root", True, True, False),
        ("https://cernbox.cern.ch/files/spaces/test.root", False, False, False),
        # Rucio dataset (implicit)
        ("test:data", True, False, False),
        ("test:data", False, False, False),
        # Rucio dataset (explicit)
        ("rucio://test:test", True, False, False),
        ("rucio://test:test", False, False, False),
    ],
)
def test_find_dataset(fs, input_path, prefer_local, expected_local, fs_file):
    if fs_file:
        fs.create_file("/data/data.root")

    dataset, use_local = find_dataset(input_path, prefer_local)
    assert use_local is expected_local


@pytest.mark.parametrize(
    "input_path, expected_message",
    [
        ("/data/test.root", "looks like a file path"),  # non-existent path
        ("test.root", "missing a Rucio namespace"),  # ambiguous file name
        ("file:///data/test.root", "Local file"),  # file:// that doesn’t exist
    ],
)
def test_find_dataset_errors(input_path, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        find_dataset(input_path)


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
    codegen_name, adaptor = install_sx_local(image, platform)
    assert isinstance(adaptor.science_runner, expected_class)


def test_install_sx_local_errors():
    with pytest.raises(ValueError, match="Unknown platform"):
        install_sx_local("some_image", platform="invalid_platform")
