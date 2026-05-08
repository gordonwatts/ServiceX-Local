import getpass
import hashlib
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, List

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


def _get_cache_file(cache_dir: Path) -> Path:
    """
    Get the path to the cache file.

    Args:
        cache_dir (Path): The directory where the cache file is stored.
    Returns:
        Path: The path to the cache file.
    """
    return cache_dir / "cache.json"


def _load_cache(cache_dir: Path) -> dict[str, Any]:
    """
    Load the cache from the file system.

    Returns:
        dict[str, Any]: The cache dictionary.
    """
    if not _get_cache_file(cache_dir).exists():
        return {}
    with _get_cache_file(cache_dir).open("r") as f:
        return json.load(f)


def _save_cache(cache: dict[str, Any], cache_dir: Path) -> None:
    """
    Save the cache to the file system.

    Args:
        cache (dict[str, Any]): The cache dictionary.
        cache_dir (Path): The directory where the cache file is stored.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    with _get_cache_file(cache_dir).open("w") as f:
        json.dump(cache, f)


async def deliver_async(
    spec: ServiceXSpec,
    adaptor: SXLocalAdaptor,
    ignore_local_cache: bool = False,
    **kwargs,
) -> dict[str, GuardList] | None:

    results: dict[str, GuardList] = {}
    cache = _load_cache(adaptor.cache_dir)  # Load cache from file system

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
                k.replace("_", "-") if k not in ("request_id", "did_id") else k: v
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
            _save_cache(cache, adaptor.cache_dir)

        # Build the list of results.
        minio_results = MinioLocalAdaptor.for_transform(status)
        download_dir = adaptor.cache_dir / status.request_id
        files = [
            await minio_results.download_file(n.filename, download_dir)
            for n in await minio_results.list_bucket()
        ]
        outputs = GuardList(files)

        title = tq.title if tq.title is not None else "local-run-dataset"
        results[title] = outputs

    return results


deliver = make_sync(deliver_async)
