import logging
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from servicex import ResultDestination
from servicex.models import (
    ResultFormat,
    Status,
    TransformRequest,
    TransformStatus,
)

from servicex_local import (
    SXLocalAdaptor,
)
from servicex_local.adaptor import MinioLocalAdaptor


def test_adaptor_url():
    codegen = MagicMock()
    science_runner = MagicMock()
    url = "http://localhost:5000"
    adaptor = SXLocalAdaptor(codegen, science_runner, "mock_codegen", url)

    assert adaptor.url == url


@pytest.mark.asyncio
async def test_adaptor_submit_transform_one_file(
    science_runner_one_txt_file: MagicMock,
):
    # Create mock objects for code generator and science image
    mock_codegen = MagicMock()

    # Configure the mock code generator to do nothing
    mock_codegen.gen_code = MagicMock()

    # Create the SXLocalAdaptor with the mock objects
    adaptor = SXLocalAdaptor(
        mock_codegen,
        science_runner_one_txt_file,
        "mock_codegen",
        "http://localhost:5000",
    )

    # Create a TransformRequest
    transform_request = TransformRequest(
        **{
            "selection": "dummy_selection",
            "file-list": ["input_file.root"],
            "result_format": ResultFormat.root_ttree,
            "result_destination": ResultDestination.volume,
            "codegen": "dummy",
        }
    )

    # Call submit_transform and get the request ID
    request_id = await adaptor.submit_transform(transform_request)

    # Verify the request ID is a valid UUID
    assert uuid.UUID(request_id)

    # Verify the transform status is stored correctly
    transform_status = await adaptor.get_transform_status(request_id)
    assert transform_status.request_id == request_id
    assert transform_status.status == Status.complete
    assert transform_status.files_completed == 1
    assert transform_status.files_failed == 0
    assert transform_status.files_remaining == 0
    assert transform_status.files == 1

    # Verify the mock methods were called correctly
    mock_codegen.gen_code.assert_called_once()

    # Verify the output directory contains one file
    output_directory = Path(tempfile.gettempdir()) / f"servicex/{request_id}"
    output_files = list(output_directory.glob("*"))
    assert len(output_files) == 1
    assert output_files[0].name == "output_file.txt"


@pytest.fixture()
def code_gen_one_windows_file() -> MagicMock:
    "return a code generator mock that will write a Windows line ending file"

    def generate_files(query: str, directory: Path):
        bad_file = directory / "bad_file.sh"
        with bad_file.open("wb") as h_bad_file:
            h_bad_file.write(b"echo 'Hello, world!'\r\necho 'no'\r\n")
        return bad_file

    code_generator = MagicMock()
    code_generator.gen_code = generate_files

    return code_generator


@pytest.fixture()
def code_gen_one_file() -> MagicMock:
    "return a code generator mock that will write a regular source file"

    def generate_files(query: str, directory: Path):
        f = directory / "run_me.sh"
        with f.open("w") as h:
            h.writelines(["hi", "there"])
        return f

    code_generator = MagicMock()
    code_generator.gen_code = generate_files

    return code_generator


@pytest.fixture()
def code_gen_error() -> MagicMock:
    "Fail while writing a code-gen, and make sure that the file is actually written out."

    def generate_files(query: str, directory: Path):
        f = directory / "run_me.sh"
        with f.open("w") as h:
            h.writelines(["hi", "there"])

        raise RuntimeError("This is a code generator error")

    code_generator = MagicMock()
    code_generator.gen_code = generate_files

    return code_generator


@pytest.fixture()
def science_runner_one_txt_file() -> MagicMock:
    mock_science_runner = MagicMock()

    def mock_transform(
        generated_files_dir, input_files, output_directory: Path, output_format
    ):
        output_file = output_directory / "output_file.txt"
        output_file.write_text("Hello, world!")

        # Check all .sh files in the generated_files_dir directory for anything with linux line
        # endings. If we find one, we should assert.
        for file in generated_files_dir.glob("*.sh"):
            with file.open("rb") as f:
                text = f.read().decode("utf-8")
                assert (
                    "\r\n" not in text
                ), "No Windows line endings allowed in a sh file"

        return [output_file]

    mock_science_runner.transform = mock_transform
    return mock_science_runner


@pytest.mark.asyncio
async def test_adaptor_submit_transform_line_endings(
    code_gen_one_windows_file, science_runner_one_txt_file
):
    """Write a source .sh file that has Windows line endings and check in the
    science container that it is Linux line endings"""

    # Create the SXLocalAdaptor with the mock objects
    adaptor = SXLocalAdaptor(
        code_gen_one_windows_file,
        science_runner_one_txt_file,
        "mock_codegen",
        "http://localhost:5000",
    )

    # Create a TransformRequest
    transform_request = TransformRequest(
        **{
            "selection": "dummy_selection",
            "file-list": ["input_file.root"],
            "result_format": ResultFormat.root_ttree,
            "result_destination": ResultDestination.volume,
            "codegen": "dummy",
        }
    )

    # Call submit_transform and get the request ID
    request_id = await adaptor.submit_transform(transform_request)

    # Verify the transform status is stored correctly
    transform_status = await adaptor.get_transform_status(request_id)
    assert transform_status.files == 1


@pytest.mark.asyncio
async def test_adaptor_saved_code_on_no_error(
    code_gen_one_file, science_runner_one_txt_file, caplog
):
    "By default make sure code is not saved even with error"

    # Create the SXLocalAdaptor with the mock objects
    adaptor = SXLocalAdaptor(
        code_gen_one_file,
        science_runner_one_txt_file,
        "mock_codegen",
        "http://localhost:5000",
    )

    # Create a TransformRequest
    transform_request = TransformRequest(
        **{
            "selection": "dummy_selection",
            "file-list": ["input_file.root"],
            "result_format": ResultFormat.root_ttree,
            "result_destination": ResultDestination.volume,
            "codegen": "dummy",
        }
    )

    # Capture the log output
    with caplog.at_level(logging.ERROR):
        await adaptor.submit_transform(transform_request)

    # Make sure nothing was written out.
    log_messages = caplog.text
    assert not any(
        "Error during transformation" in msg for msg in log_messages
    ), "Unexpected error message found in logs"


@pytest.mark.asyncio
async def test_adaptor_saved_code_on_error(
    code_gen_error, science_runner_one_txt_file, caplog
):
    "By default make sure code is not saved even with error"

    # Create the SXLocalAdaptor with the mock objects
    adaptor = SXLocalAdaptor(
        code_gen_error,
        science_runner_one_txt_file,
        "mock_codegen",
        "http://localhost:5000",
    )

    # Create a TransformRequest
    transform_request = TransformRequest(
        **{
            "selection": "dummy_selection",
            "file-list": ["input_file.root"],
            "result_format": ResultFormat.root_ttree,
            "result_destination": ResultDestination.volume,
            "codegen": "dummy",
        }
    )

    # Capture the log output
    with pytest.raises(RuntimeError):
        with caplog.at_level(logging.ERROR):
            await adaptor.submit_transform(transform_request)

    # Extract the directory from the log error message
    log_messages = caplog.text
    error_message = next(
        (
            msg
            for msg in log_messages.splitlines()
            if "Error during transformation" in msg
        ),
        None,
    )
    assert error_message is not None, "Expected error message not found in logs"
    directory_path = error_message.split("found at: ")[1].strip()
    d = Path(directory_path)
    assert d.exists()
    assert (d / "run_me.sh").exists()


def create_transform_status(request_id: str) -> TransformStatus:
    return TransformStatus(
        **{
            "request_id": request_id,
            "minio_endpoint": "localhost",
            "minio_secured": False,
            "minio_access_key": "access_key",
            "minio_secret_key": "secret_key",
            "did": "123",
            "selection": "dummy_selection",
            "tree-name": "dummy_tree",
            "image": "dummy_image",
            "result-destination": ResultDestination.volume,
            "result-format": ResultFormat.root_ttree,
            "generated-code-cm": "dummy_codegen",
            "status": Status.complete,
            "app-version": "dummy_version",
            "files-completed": 1,
            "files-failed": 0,
            "files": 1,
        }
    )


@pytest.mark.asyncio
async def test_minio_list_bucket():

    # Create a MinioLocalAdaptor instance
    transform_status = create_transform_status("test_request_id")
    adaptor = MinioLocalAdaptor.for_transform(transform_status)

    # Mock the output directory to point to the temporary directory
    adaptor.request_id = "test_request_id"
    output_directory = Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "file1.txt").write_text("content1")
    (output_directory / "file2.txt").write_text("content2")

    # Call list_bucket and verify the result
    result_files = await adaptor.list_bucket()
    assert len(result_files) == 2
    assert result_files[0].filename in ["file1.txt", "file2.txt"]
    assert result_files[1].filename in ["file1.txt", "file2.txt"]
    assert result_files[0].filename != result_files[1].filename


@pytest.mark.asyncio
async def test_minio_download_file():
    # Create a MinioLocalAdaptor instance
    transform_status = create_transform_status("test_request_id")
    adaptor = MinioLocalAdaptor.for_transform(transform_status)

    # Mock the output directory to point to the temporary directory
    adaptor.request_id = "test_request_id"
    output_directory = Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "file1.txt").write_text("content1")

    # Call download_file and verify the result
    local_dir = Path(tempfile.gettempdir()) / "local_dir"
    downloaded_file = await adaptor.download_file("file1.txt", str(local_dir))
    assert downloaded_file.exists()
    assert downloaded_file.read_text() == "content1"


@pytest.mark.asyncio
async def test_minio_get_signed_url():
    # Create a MinioLocalAdaptor instance
    transform_status = create_transform_status("test_request_id")
    adaptor = MinioLocalAdaptor.for_transform(transform_status)

    # Mock the output directory to point to the temporary directory
    adaptor.request_id = "test_request_id"
    output_directory = Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "file1.txt").write_text("content1")

    # Call get_signed_url and verify the result
    signed_url = await adaptor.get_signed_url("file1.txt")
    assert signed_url.startswith("file://")
    assert signed_url.endswith("file1.txt")
