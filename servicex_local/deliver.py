import hashlib
import os
import json
import tempfile
from typing import Any, Generator, List
from pathlib import Path

from make_it_sync import make_sync
from servicex import General, ResultDestination, Sample, ServiceXSpec
from servicex.models import ResultFormat, TransformRequest
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
        samples (List[Sample]): A list of Sample objects containing information about each sample.
    Yields:
        TransformRequest: A TransformRequest object for each sample in the list.
    """
    for s in samples:

        tq = TransformRequest(
            title=s.Name,
            codegen="local-codegen",
            selection=str(s.Query),
            result_destination=ResultDestination.object_store,
            result_format=ResultFormat.root_ttree,
        )

        s.dataset_identifier.populate_transform_request(tq)

        yield tq


def _generate_cache_key(tq: TransformRequest) -> str:
    """
    Generate a cache key based on the file_list and selection of the TransformRequest.

    Args:
        tq (TransformRequest): The TransformRequest object.
    Returns:
        str: A hash string representing the cache key.
    """
    key = f"{tq.file_list}-{tq.selection}"
    return hashlib.md5(key.encode()).hexdigest()


CACHE_DIR = Path(tempfile.gettempdir()) / "servicex"
CACHE_FILE = CACHE_DIR / "cache.json"


def _load_cache() -> dict[str, str]:
    """
    Load the cache from the file system.

    Returns:
        dict[str, str]: The cache dictionary.
    """
    if not CACHE_FILE.exists():
        return {}
    with CACHE_FILE.open("r") as f:
        return json.load(f)


def _save_cache(cache: dict[str, str]) -> None:
    """
    Save the cache to the file system.

    Args:
        cache (dict[str, str]): The cache dictionary.
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
            request_id = cache[cache_key]
        else:
            # Do the transform and get status
            request_id = await adaptor.submit_transform(tq)
            cache[cache_key] = request_id
            _save_cache(cache)  # Save cache to file system

        status = await adaptor.get_transform_status(request_id)

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
