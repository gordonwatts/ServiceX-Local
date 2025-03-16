from datetime import datetime
import tempfile

import pytest
from servicex import General, ResultDestination, Sample, ServiceXSpec, dataset
from servicex.models import ResultFormat, Status, TransformRequest, TransformStatus
from servicex_local import deliver
import os
from pathlib import Path
import uuid


@pytest.fixture
def simple_adaptor():
    class my_adaptor:
        def __init__(self):
            self._request_id = None

        async def submit_transform(self, tq: TransformRequest) -> str:
            self._request_id = str(uuid.uuid4())

            file_path = (
                Path(tempfile.gettempdir())
                / "servicex"
                / self._request_id
                / "file1.root"
            )
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch()

            return self._request_id

        async def get_transform_status(self, request_id: str):
            assert request_id == self._request_id
            return TransformStatus(
                **{
                    "did": "file1",
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


def test_deliver_spec_simple(simple_adaptor):
    "Test a simple deliver that should work"

    spec = ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="test_me", Dataset=dataset.FileList("test.root"), Query="query1"
            )
        ],
    )

    r = deliver(spec, adaptor=simple_adaptor)
    assert r is not None
    assert len(r) == 1
    assert "test_me" in r
    files = r["test_me"]
    assert len(files) == 1
    local_path = Path(files[0].replace("file:///", ""))
    assert os.path.exists(local_path)


# test for warning for extra arguments that aren't known.
