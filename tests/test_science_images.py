import os
import shutil
from pathlib import Path

import pytest

from servicex_local.science_images import DockerScienceImage, WSL2ScienceImage


@pytest.mark.skip(reason="This test needs docker to be installed")
def test_docker_science(tmp_path):
    "Run a simple test of the docker science image"

    # We need the files we'll use as input.
    generated_file_directory = tmp_path / "input"
    generated_file_directory.mkdir()
    output_file_directory = tmp_path / "output"
    output_file_directory.mkdir()

    source_directory = "./tests/genfiles_raw/query1_raw"
    for file_name in os.listdir(source_directory):
        full_file_name = os.path.join(source_directory, file_name)
        if os.path.isfile(full_file_name):
            shutil.copy(full_file_name, generated_file_directory)

    # Now we can run the science image

    input_files = [
        "root://fax.mwt2.org:1094//pnfs/uchicago.edu/atlaslocalgroupdisk/"
        "rucio/user/mgeyik/e7/ee/user.mgeyik.30182995._000093.out.root"
    ]
    docker = DockerScienceImage("sslhep/servicex_func_adl_uproot_transformer:uproot5")
    output_files = docker.transform(
        generated_file_directory, input_files, output_file_directory, "root-file"
    )

    assert len(output_files) == 1
    assert output_files[0].exists()


# @pytest.mark.skip(reason="This test needs wsl2 to be installed")
def test_wsl2_science(tmp_path):
    """Test a xAOD transform on a WSL2 atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """

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
