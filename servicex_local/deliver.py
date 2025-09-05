import getpass
import hashlib
import json
import tempfile
from typing import Any, Generator, List
from pathlib import Path
from datetime import datetime

from make_it_sync import make_sync
from servicex import General, ResultDestination, Sample, ServiceXSpec
from servicex.models import ResultFormat, TransformRequest, TransformStatus
from servicex.query_core import QueryStringGenerator
from servicex.servicex_client import GuardList
from servicex_local import SXLocalAdaptor
from servicex_local.adaptor import MinioLocalAdaptor


def _sample_run_info(
    g: General, samples: List[Sample]
) -> Generator[TransformRequest, Any, None]:
    """
    Generate TransformRequest objects for a list of samples.

    Args:
        g (General): A general configuration object.
        samples (List[Sample]): A list of Sample objects containing information
            about each sample.
    Yields:
        TransformRequest:
            A TransformRequest object for each sample in the list.
    """
    for s in samples:
        selection = (
            s.Query
            if isinstance(s.Query, str)
            else (
                s.Query.generate_selection_string()
                if isinstance(s.Query, QueryStringGenerator)
                else None
            )
        )
        assert (
            selection is not None
        ), f"Unable to translate query {s.Query} into a string"

        tq = TransformRequest(
            title=s.Name,
            codegen="local-codegen",
            selection=selection,
            result_destination=ResultDestination.object_store,
            result_format=ResultFormat.root_ttree,
        )

        s.dataset_identifier.populate_transform_request(tq)

        yield tq


def _generate_cache_key(tq: TransformRequest) -> str:
    """
    Generate a cache key based on the file_list and selection of the
    TransformRequest.

    Args:
        tq (TransformRequest): The TransformRequest object.
    Returns:
        str: A hash string representing the cache key.
    """
    key = f"{tq.file_list}-{tq.selection}"
    return hashlib.md5(key.encode()).hexdigest()


CACHE_DIR: Path = Path(tempfile.gettempdir()) / f"servicex_{getpass.getuser()}"
CACHE_FILE: Path = CACHE_DIR / "cache.json"


def _load_cache() -> dict[str, Any]:
    """
    Load the cache from the file system.

    Returns:
        dict[str, Any]: The cache dictionary.
    """
    if not CACHE_FILE.exists():
        return {}
    with CACHE_FILE.open("r") as f:
        return json.load(f)


def _save_cache(cache: dict[str, Any]) -> None:
    """
    Save the cache to the file system.

    Args:
        cache (dict[str, Any]): The cache dictionary.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with CACHE_FILE.open("w") as f:
        json.dump(cache, f)


async def deliver_async(
    spec: ServiceXSpec,
    adaptor: SXLocalAdaptor,
    ignore_local_cache: bool = False,
    **kwargs,
) -> dict[str, GuardList] | None:

    results: dict[str, GuardList] = {}
    cache = _load_cache()  # Load cache from file system

    # Run the samples one by one.
    for tq in _sample_run_info(spec.General, spec.Sample):
        cache_key = _generate_cache_key(tq)

        if cache_key in cache and not ignore_local_cache:
            info = cache[cache_key]
            info["submit_time"] = datetime.fromisoformat(info["submit_time"])
            info["finish_time"] = (
                datetime.fromisoformat(info["finish_time"])
                if info["finish_time"] is not None
                else None
            )
            info = {
                k.replace("_", "-") if k != "request_id" else k: v
                for k, v in info.items()
            }

            status = TransformStatus(**info)
        else:
            # Do the transform and get status
            request_id = await adaptor.submit_transform(tq)
            status = await adaptor.get_transform_status(request_id)
            info = status.model_dump()
            info["submit_time"] = info["submit_time"].isoformat()
            info["finish_time"] = (
                info["finish_time"].isoformat()
                if info["finish_time"] is not None
                else None
            )

            cache[cache_key] = info
            _save_cache(cache)  # Save cache to file system

        # Build the list of results.
        minio_results = MinioLocalAdaptor.for_transform(status)
        files = [
            await minio_results.get_signed_url(n.filename)
            for n in await minio_results.list_bucket()
        ]
        outputs = GuardList(files)

        title = tq.title if tq.title is not None else "local-run-dataset"
        results[title] = outputs

    return results


deliver = make_sync(deliver_async)
