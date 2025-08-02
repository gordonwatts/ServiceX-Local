import logging
import os
import shutil
from pathlib import Path
from typing import List

import pytest

from servicex_local.science_images import (
    DockerScienceImage,
    WSL2ScienceImage,
    SingularityScienceImage,
)


def prepare_input_files(
    tmp_path: Path, source_directory: str, input_files: List[str]
) -> tuple[Path, List[str], Path]:
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
        if (
            not file.startswith("root://")
            and not file.startswith("https://")
            and not file.startswith("http://")
        ):
            (input_data_directory / file).touch()
            actual_input_files.append(str(input_data_directory / file))
        else:
            actual_input_files.append(file)

    return generated_file_directory, actual_input_files, output_file_directory


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
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "sslhep/servicex_func_adl_xaod_transformer:25.2.41",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root", "file2.root"],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["http://root.ch/file1"],
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["https://root.ch/file1"],
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
def test_docker_science(
    tmp_path, request, source_directory, input_files, container_name
):
    """Test against a docker science image - integrated (uses docker)
    WARNING: This expects to find the x509 cert!!!
    """
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(tmp_path, source_directory, input_files)
    )

    # Now we can run the science image
    docker = DockerScienceImage(container_name)
    logging.basicConfig(level=logging.DEBUG)
    output_files = docker.transform(
        generated_file_directory, actual_input_files, output_file_directory, "root-file"
    )

    assert len(output_files) == len(actual_input_files)
    assert all(o.exists() for o in output_files)


@pytest.mark.parametrize(
    "source_directory, input_files, container_name",
    [
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "docker://sslhep/servicex_func_adl_xaod_transformer:21.2.231",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "docker://sslhep/servicex_func_adl_xaod_transformer:21.2.231",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "docker://sslhep/servicex_func_adl_xaod_transformer:25.2.41",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root", "file2.root"],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["http://root.ch/file1"],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["https://root.ch/file1"],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            [
                "root://fax.mwt2.org:1094//pnfs/uchicago.edu/atlaslocalgroupdisk/"
                "rucio/user.mgeyik/e7/ee/user.mgeyik.30182995._000093.out.root"
            ],
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5",
        ),
    ],
)
def test_singularity_science(
    tmp_path, request, source_directory, input_files, container_name
):
    """Test against a Singularity science image - integrated (uses Singularity)
    WARNING: This expects to find the x509 cert!!!
    """
    if not request.config.getoption("--singularity"):
        pytest.skip("Use the --singularity pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(tmp_path, source_directory, input_files)
    )

    # Now we can run the science image
    docker = SingularityScienceImage(container_name)
    logging.basicConfig(level=logging.DEBUG)
    output_files = docker.transform(
        generated_file_directory, actual_input_files, output_file_directory, "root-file"
    )

    assert len(output_files) == len(actual_input_files)
    assert all(o.exists() for o in output_files)


def test_singularity_science_logging(tmp_path, caplog, request):
    """Run a simple singularity transform and make sure we pick up log messages."""
    if not request.config.getoption("--singularity"):
        pytest.skip("Use the --singularity pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query6_logging", ["file1.root"]
        )
    )

    with caplog.at_level(logging.DEBUG):
        docker = SingularityScienceImage(
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5"
        )
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    assert "this is log line 2" in caplog.text


def test_singularity_science_log_warnings(tmp_path, caplog, request):
    """Run a simple singularity transform and make sure we pick up warning or error log messages."""
    if not request.config.getoption("--singularity"):
        pytest.skip("Use the --singularity pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query7_logging_warnings", ["file1.root"]
        )
    )

    with caplog.at_level(logging.WARNING):
        docker = SingularityScienceImage(
            "docker://sslhep/servicex_func_adl_uproot_transformer:uproot5"
        )
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    assert "this is log line 2" in caplog.text
    assert "this is log line 1" in caplog.text

    # Make sure these lines also appear in the logger output!
    written_log = (generated_file_directory / "singularity_log.txt").read_text()
    assert "this is log line 2" in written_log


@pytest.mark.parametrize(
    "source_directory, input_files, release, wsl_distro",
    [
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "24.2.41",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root"],
            "21.2.231",
            "atlas_centos7",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "24.2.41",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "24.2.41",
            "atlas_centos7",
        ),
        (
            "./tests/genfiles_raw/query1_python",
            ["file1.root"],
            "25.2.12",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["file1.root", "file2.root"],
            "24.2.41",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            [
                "root://fax.mwt2.org:1094//pnfs/uchicago.edu/atlaslocalgroupdisk/"
                "rucio/user.mgeyik/e7/ee/user.mgeyik.30182995._000093.out.root"
            ],
            "24.2.41",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["http://root.ch/file1.root"],
            "24.2.41",
            "atlas_al9",
        ),
        (
            "./tests/genfiles_raw/query2_bash",
            ["https://root.ch/file1.root"],
            "24.2.41",
            "atlas_al9",
        ),
    ],
)
def test_wsl2_science(
    tmp_path, request, source_directory, input_files, release, wsl_distro
):
    """Test against a docker science image - integrated (uses docker)
    WARNING: This expects to find the x509 cert!!!
    """
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(tmp_path, source_directory, input_files)
    )

    # Now we can run the science image
    wsl2 = WSL2ScienceImage(wsl_distro, release)
    logging.basicConfig(level=logging.DEBUG)
    output_files = wsl2.transform(
        generated_file_directory, actual_input_files, output_file_directory, "root-file"
    )

    assert len(output_files) == len(actual_input_files)
    assert all(o.exists() for o in output_files)


def test_wsl2_science_logging(tmp_path, caplog, request):
    """Run a simple wsl2 transform and make sure we pick up log messages."""
    if not request.config.getoption("--wsl2"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query6_logging", ["file1.root"]
        )
    )

    with caplog.at_level(logging.DEBUG):
        wsl2 = WSL2ScienceImage("atlas_al9", "25.2.12")
        wsl2.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    assert "this is log line 2" in caplog.text


def test_docker_science_logging(tmp_path, caplog, request):
    """Run a simple docker transform and make sure we pick up log messages."""
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query6_logging", ["file1.root"]
        )
    )

    with caplog.at_level(logging.DEBUG):
        docker = DockerScienceImage(
            "sslhep/servicex_func_adl_uproot_transformer:uproot5"
        )
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    assert "this is log line 2" in caplog.text


def test_docker_science_log_warnings(tmp_path, caplog, request):
    """Run a simple docker transform and make sure we pick up warning or error log messages."""
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query7_logging_warnings", ["file1.root"]
        )
    )

    with caplog.at_level(logging.WARNING):
        docker = DockerScienceImage(
            "sslhep/servicex_func_adl_uproot_transformer:uproot5"
        )
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    assert "this is log line 2" in caplog.text
    assert "this is log line 1" in caplog.text

    # Make sure these lines also appear in the logger output!
    written_log = (generated_file_directory / "docker_log.txt").read_text()
    assert "this is log line 2" in written_log


@pytest.mark.parametrize(
    "wsl_distro, release, transform_path, exception_message",
    [
        (
            "atlas_al9",
            "25.2.12",
            "tests/genfiles_raw/query3_bash_exit_error",
            "exit_code=10",
        ),
        (
            "atlas_al9",
            "25.2.12",
            "tests/genfiles_raw/query4_python_exit_error",
            "exit_code=10",
        ),
        (
            "atlas_al9",
            "25.2.12",
            "tests/genfiles_raw/query5_python_exception_error",
            "exit_code=1",
        ),
    ],
)
def test_wsl2_science_error(
    tmp_path, request, wsl_distro, release, transform_path, exception_message
):
    """Test a xAOD transform on a WSL2 atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """
    if not request.config.getoption("--wsl2"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(tmp_path, transform_path, ["file1.root"])
    )

    wsl2 = WSL2ScienceImage(wsl_distro, release)
    with pytest.raises(
        RuntimeError,
        match=(exception_message),
    ):
        wsl2.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )


@pytest.mark.parametrize(
    "container_name, transform_path, exception_message",
    [
        (
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
            "tests/genfiles_raw/query3_bash_exit_error",
            "exit_code=10",
        ),
        (
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
            "tests/genfiles_raw/query4_python_exit_error",
            "exit_code=10",
        ),
        (
            "sslhep/servicex_func_adl_uproot_transformer:uproot5",
            "tests/genfiles_raw/query5_python_exception_error",
            "exit_code=1",
        ),
    ],
)
def test_docker_science_error(
    tmp_path, request, container_name, transform_path, exception_message
):
    """Test a xAOD transform on a docker atlas distribution
    This test takes about 100 seconds to run on a connection
    that is reasonable (at home). Takes 300 to 400 seconds if
    cvmfs is cold.
    """
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(tmp_path, transform_path, ["file1.root"])
    )

    docker = DockerScienceImage(container_name)
    with pytest.raises(
        RuntimeError,
        match=(exception_message),
    ):
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )


def test_docker_stderr_ordering(tmp_path, caplog, request):
    "Make sure that we can deal with stderr and stdout messages interleaved"
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    generated_file_directory, actual_input_files, output_file_directory = (
        prepare_input_files(
            tmp_path, "tests/genfiles_raw/query8_stderr", ["file1.root"]
        )
    )

    with caplog.at_level(logging.DEBUG):
        docker = DockerScienceImage(
            "sslhep/servicex_func_adl_uproot_transformer:uproot5"
        )
        docker.transform(
            generated_file_directory,
            actual_input_files,
            output_file_directory,
            "root-file",
        )

    expected_messages = [
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stdout",
        "This is a test message to stdout",
        "This is a test message to stdout",
        "This is a test message to stdout",
        "This is a test message to stdout",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stderr",
        "This is a test message to stdout",
    ]

    log_messages = [record.message for record in caplog.records]
    for log_message in log_messages:
        if expected_messages[0] in log_message:
            expected_messages.pop(0)
        if len(expected_messages) == 0:
            break

    assert len(expected_messages) == 0
