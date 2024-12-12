import logging
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from servicex import ResultDestination, dataset, deliver
from servicex import query as q
from servicex.models import (
    ResultFormat,
    Status,
    TransformRequest,
    TransformStatus,
    ResultFile,
)

from servicex_local import LocalXAODCodegen, SXLocalAdaptor, WSL2ScienceImage
from servicex_local.adaptor import MinioLocalAdaptor


def test_adaptor_xaod_wsl2():
    codegen = LocalXAODCodegen()
    science_runner = WSL2ScienceImage("atlas_al9", "25.2.12")
    adaptor = SXLocalAdaptor(
        codegen, science_runner, "atlasr22", "http://localhost:5001"
    )
    minio = MinioLocalAdaptor("my_bucket")

    logging.basicConfig(level=logging.DEBUG)

    # The simple query, take straight from the example in the documentation.
    query = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets("AnalysisJets"))
    jet_info_per_event = jets_per_event.Select(
        lambda jets: {
            "pt": jets.Select(lambda j: j.pt()),
            "eta": jets.Select(lambda j: j.eta()),
        }
    )

    spec = {
        "Sample": [
            {
                "Name": "func_adl_xAOD_simple",
                "Dataset": dataset.FileList(
                    [
                        "root://eospublic.cern.ch//eos/opendata/atlas/rucio/mc20_13TeV/"
                        "DAOD_PHYSLITE.37622528._000013.pool.root.1"
                    ]
                ),
                "Query": jet_info_per_event,
            }
        ]
    }
    files = deliver(
        spec,
        servicex_name="test-backend",
        servicex_adaptor=adaptor,  # type: ignore
        minio_adaptor_class=MinioLocalAdaptor,
    )
    assert files is not None, "No files returned from deliver! Internal error"
    assert False


def test_adaptor_url():
    codegen = MagicMock()
    science_runner = MagicMock()
    url = "http://localhost:5000"
    adaptor = SXLocalAdaptor(codegen, science_runner, "mock_codegen", url)

    assert adaptor.url == url


@pytest.mark.asyncio
async def test_submit_transform():
    # Create mock objects for code generator and science image
    mock_codegen = MagicMock()
    mock_science_runner = MagicMock()

    # Configure the mock code generator to do nothing
    mock_codegen.gen_code = MagicMock()

    # Configure the mock science image to write one file to the output directory
    def mock_transform(
        generated_files_dir, input_files, output_directory, output_format
    ):
        output_file = output_directory / "output_file.txt"
        output_file.write_text("dummy content")
        return [output_file]

    mock_science_runner.transform = mock_transform

    # Create the SXLocalAdaptor with the mock objects
    adaptor = SXLocalAdaptor(
        mock_codegen, mock_science_runner, "mock_codegen", "http://localhost:5000"
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
async def test_list_bucket():
    # Create a temporary directory to simulate the Minio bucket
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Create a MinioLocalAdaptor instance
        transform_status = create_transform_status("test_request_id")
        adaptor = MinioLocalAdaptor.for_transform(transform_status)

        # Mock the output directory to point to the temporary directory
        adaptor.request_id = "test_request_id"
        output_directory = (
            Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        (output_directory / "file1.txt").write_text("content1")
        (output_directory / "file2.txt").write_text("content2")

        # Call list_bucket and verify the result
        result_files = await adaptor.list_bucket()
        assert len(result_files) == 2
        assert result_files[0].filename == "file1.txt"
        assert result_files[1].filename == "file2.txt"


@pytest.mark.asyncio
async def test_download_file():
    # Create a temporary directory to simulate the Minio bucket
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Create a MinioLocalAdaptor instance
        transform_status = create_transform_status("test_request_id")
        adaptor = MinioLocalAdaptor.for_transform(transform_status)

        # Mock the output directory to point to the temporary directory
        adaptor.request_id = "test_request_id"
        output_directory = (
            Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        (output_directory / "file1.txt").write_text("content1")

        # Call download_file and verify the result
        local_dir = Path(tempfile.gettempdir()) / "local_dir"
        downloaded_file = await adaptor.download_file("file1.txt", str(local_dir))
        assert downloaded_file.exists()
        assert downloaded_file.read_text() == "content1"


@pytest.mark.asyncio
async def test_get_signed_url():
    # Create a temporary directory to simulate the Minio bucket
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir_path = Path(temp_dir)

        # Create a MinioLocalAdaptor instance
        transform_status = create_transform_status("test_request_id")
        adaptor = MinioLocalAdaptor.for_transform(transform_status)

        # Mock the output directory to point to the temporary directory
        adaptor.request_id = "test_request_id"
        output_directory = (
            Path(tempfile.gettempdir()) / f"servicex/{adaptor.request_id}"
        )
        output_directory.mkdir(parents=True, exist_ok=True)
        (output_directory / "file1.txt").write_text("content1")

        # Call get_signed_url and verify the result
        signed_url = await adaptor.get_signed_url("file1.txt")
        assert signed_url.startswith("file://")
        assert signed_url.endswith("file1.txt")
