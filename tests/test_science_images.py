import logging
import os
from re import A
import shutil
from pathlib import Path

import pytest

from servicex_local.science_images import DockerScienceImage, WSL2ScienceImage


@pytest.mark.parametrize(
    "source_directory, input_files, container_name",
    [
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "sslhep/servicex_func_adl_xaod_transformer:21.2.231",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "sslhep/servicex_func_adl_xaod_transformer:21.2.231",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root", "file2.root"],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            [
                "root://fax.mwt2.org:1094//pnfs/uchicago.edu/atlaslocalgroupdisk/"
                "rucio/user.mgeyik/e7/ee/user.mgeyik.30182995._000093.out.root"
            ],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
    ],
)
def test_docker_science_bash(
    tmp_path, request, source_directory, input_files, container_name
):
    """Test against a docker science image - integrated (uses docker)
    WARNING: This expects to find the x509 cert!!!
    """
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    # We need the files we'll use as input.
    generated_file_directory = tmp_path / "input"
    generated_file_directory.mkdir()
    output_file_directory = tmp_path / "output"
    output_file_directory.mkdir()

    for file_name in os.listdir(source_directory):
        full_file_name = os.path.join(source_directory, file_name)
        if os.path.isfile(full_file_name):
            shutil.copy(full_file_name, generated_file_directory)

    # Create input files in tmp_path/input_data if they don't start with root://
    input_data_directory = tmp_path / "input_data"
    input_data_directory.mkdir()
    actual_input_files = []
    for file in input_files:
        if not file.startswith("root://"):
            (input_data_directory / file).touch()
            actual_input_files.append(str(input_data_directory / file))
        else:
            actual_input_files.append(file)

    # Now we can run the science image
    docker = DockerScienceImage(container_name)
    logging.basicConfig(level=logging.DEBUG)
    output_files = docker.transform(
        generated_file_directory, actual_input_files, output_file_directory, "root-file"
    )

    assert len(output_files) == len(actual_input_files)
    assert all(o.exists() for o in output_files)


def test_wsl2_science(tmp_path, caplog, request):
    """Test a xAOD transform on a WSL2 atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """
    if not request.config.getoption("--wsl2"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    with caplog.at_level(logging.WARNING):
        wsl2 = WSL2ScienceImage("atlas_al9", "25.2.12")
        outputs = wsl2.transform(
            Path("tests/genfiles_raw/query2_xaod"),
            [
                "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/"
                "DAOD_PHYSLITE.37622528._000013.pool.root.1"
            ],
            tmp_path / "output",
            "root-file",
        )

    assert len(outputs) == 1
    outputs[0].exists()
    assert len(caplog.records) == 0
    assert caplog.text == ""


def test_wsl2_science_logging(tmp_path, caplog, request):
    """Test a xAOD transform on a WSL2 atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """
    if not request.config.getoption("--wsl2"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")
    with caplog.at_level(logging.DEBUG):
        wsl2 = WSL2ScienceImage("atlas_al9", "25.2.12")
        outputs = wsl2.transform(
            Path("tests/genfiles_raw/query2_xaod"),
            [
                "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/"
                "DAOD_PHYSLITE.37622528._000013.pool.root.1"
            ],
            tmp_path / "output",
            "root-file",
        )

    assert len(outputs) == 1
    outputs[0].exists()
    assert "release_setup.sh" in caplog.text


def test_wsl2_science_error(tmp_path, request):
    """Test a xAOD transform on a WSL2 atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """
    if not request.config.getoption("--wsl2"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")
    wsl2 = WSL2ScienceImage("atlas_al9", "25.2.12")
    with pytest.raises(
        RuntimeError,
        match=(
            "failed to open file root://fork.me.now//eos/opendata/atlas/rucio/mc20_13TeV"
            "/DAOD_PHYSLITE.37622528._000013.pool.root.1\nDirectInputModule"
        ),
    ):
        wsl2.transform(
            Path("tests/genfiles_raw/query2_xaod"),
            [
                "root://fork.me.now//eos/opendata/atlas/rucio/mc20_13TeV/"
                "DAOD_PHYSLITE.37622528._000013.pool.root.1"
            ],
            tmp_path / "output",
            "root-file",
        )
