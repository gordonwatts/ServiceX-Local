import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path

import pytest
from servicex import General, ResultDestination, Sample, ServiceXSpec, dataset
from servicex.models import ResultFormat, Status, TransformRequest, TransformStatus
from servicex.query_core import QueryStringGenerator

from servicex_local import deliver


@pytest.fixture(autouse=True)
def restore_cache():
    """
    Fixture to restore the cache database to its original form after each test.
    """
    cache_dir = Path(tempfile.gettempdir()) / "servicex"
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


@pytest.fixture
def simple_adaptor():
    class my_adaptor:
        def __init__(self):
            self._request_id = None
            self._submit_called = 0

        @property
        def submit_called(self):
            return self._submit_called

        async def submit_transform(self, tq: TransformRequest) -> str:
            self._request_id = str(uuid.uuid4())

            assert tq.selection == "query1"

            file_path = (
                Path(tempfile.gettempdir())
                / "servicex"
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
    local_path = Path(files[0].replace("file:///", "/"))
    if os.name == "nt":
        local_path = Path(files[0].replace("file:///", ""))
    assert os.path.exists(local_path)


def test_deliver_spec_q_string_generator(simple_adaptor):
    "Test a simple deliver that should work"

    class my_string_query(QueryStringGenerator):
        def generate_selection_string(self) -> str:
            return "query1"

    spec = ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="test_me",
                Dataset=dataset.FileList("test.root"),
                Query=my_string_query(),
            )
        ],
    )

    r = deliver(spec, adaptor=simple_adaptor)
    assert r is not None
    assert len(r) == 1
    assert "test_me" in r
    files = r["test_me"]
    assert len(files) == 1
    local_path = Path(files[0].replace("file:///", "/"))
    if os.name == "nt":
        local_path = Path(files[0].replace("file:///", ""))
    assert os.path.exists(local_path)


def test_deliver_spec_simple_cache_hit(simple_adaptor):
    "Test a simple deliver that should work"

    spec = ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="test_me", Dataset=dataset.FileList("test.root"), Query="query1"
            )
        ],
    )

    deliver(spec, adaptor=simple_adaptor)
    deliver(spec, adaptor=simple_adaptor)

    assert simple_adaptor.submit_called == 1


def test_deliver_spec_simple_cache_ignore(simple_adaptor):
    "Test a simple deliver that should work"

    spec = ServiceXSpec(
        General=General(),
        Sample=[
            Sample(
                Name="test_me", Dataset=dataset.FileList("test.root"), Query="query1"
            )
        ],
    )

    deliver(spec, adaptor=simple_adaptor)
    deliver(spec, adaptor=simple_adaptor, ignore_local_cache=True)

    assert simple_adaptor.submit_called == 2


# test for warning for extra arguments that aren't known.
