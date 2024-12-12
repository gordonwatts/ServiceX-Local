import logging
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from servicex import ResultDestination, dataset, deliver
from servicex import query as q
from servicex.models import ResultFormat, Status, TransformRequest

from servicex_local import LocalXAODCodegen, SXLocalAdaptor, WSL2ScienceImage


@pytest.mark.skip(reason="This integration test is not ready to run")
def test_adaptor_xaod_wsl2():
    codegen = LocalXAODCodegen()
    science_runner = WSL2ScienceImage("atlas_al9", "22.2.107")
    adaptor = SXLocalAdaptor(codegen, science_runner)

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
                        "tests/test.root",  # noqa: E501
                    ]
                ),
                "Query": jet_info_per_event,
            }
        ]
    }
    files = deliver(spec, servicex_name="servicex-uc-af", sx_adaptor=adaptor)
    assert files is not None, "No files returned from deliver! Internal error"
    assert False


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
    adaptor = SXLocalAdaptor(mock_codegen, mock_science_runner, "mock_codegen")

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
