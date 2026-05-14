import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Generator, List, Union, Mapping
from deprecated import deprecated

from make_it_sync import make_sync
from servicex import General, ResultDestination, Sample, ServiceXSpec
from servicex.expandable_progress import ExpandableProgress
from servicex.models import ResultFormat, TransformRequest, TransformStatus
from servicex.query_core import QueryStringGenerator
from servicex.servicex_client import GuardList
from servicex.yaml_parser import YAML

from .adaptor import SXLocalAdaptor, MinioLocalAdaptor
from .codegen import LocalXAODCodegen
from .configurations import Config, Platform
from servicex_analysis_utils import to_awk

logger = logging.getLogger(__name__)


def _load_ServiceXSpec(
    config: Union[ServiceXSpec, Mapping[str, Any], str, Path],
) -> ServiceXSpec:
    if isinstance(config, Mapping):
        logger.debug("Config from dictionary")
        config = ServiceXSpec(**config)
    elif isinstance(config, ServiceXSpec):
        logger.debug("Config from ServiceXSpec")
    elif isinstance(config, str) or isinstance(config, Path):
        logger.debug("Config from file")

        if isinstance(config, str):
            file_path = Path(config)
        else:
            file_path = config

        import sys

        yaml = YAML()

        if sys.version_info < (3, 10):
            from importlib_metadata import entry_points
        else:
            from importlib.metadata import entry_points

        plugins = entry_points(group="servicex.query")
        for _ in plugins:
            yaml.register_class(_.load())
        plugins = entry_points(group="servicex.dataset")
        for _ in plugins:
            yaml.register_class(_.load())

        conf = yaml.load(file_path)
        config = ServiceXSpec(**conf)
    else:
        raise TypeError(f"Unknown config type: {type(config)}")

    return config


def install_sx_local(
    image: str, platform: Platform = Platform.docker, host_port: int = 5001
):
    """Set up a local ServiceX endpoint for data transformation.

    Args:
        image (str): Image name for the container.
        platform (Platform): Which platform to use.
        host_port (int): Local host port to expose.

    Returns:
        Tuple[str, SXLocalAdaptor]: Codegen name, adaptor.
    """
    from servicex.configuration import Configuration

    try:
        sx_cfg = Configuration.read()
        cache_dir = Path(sx_cfg.cache_path).resolve()
    except NameError:
        import tempfile

        cache_dir = Path(tempfile.mkdtemp()).resolve()
        logging.warning(
            "Could not read a ServiceX.yaml. Using temporary directory %s for cache.",
            cache_dir,
        )

    codegen = LocalXAODCodegen()

    if platform == Platform.docker:
        from .science_images import DockerScienceImage

        science_runner = DockerScienceImage(image)

    elif platform == Platform.singularity:
        from .science_images import SingularityScienceImage

        science_runner = SingularityScienceImage(image)

    elif platform == Platform.wsl2:
        from .science_images import WSL2ScienceImage

        container, release = image.split(":")
        science_runner = WSL2ScienceImage(container, release)

    else:
        raise ValueError(f"Unknown platform {platform}")

    adaptor = SXLocalAdaptor(
        codegen, science_runner, cache_dir, f"http://localhost:{host_port}"
    )

    logging.info(f"Using local ServiceX endpoint: {codegen}")
    logging.info(f"Cache being save to; {adaptor.cache_dir}")
    return adaptor


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
    spec: Union[ServiceXSpec, Mapping[str, Any], str, Path],
    adaptor: SXLocalAdaptor,
    ignore_local_cache: bool = False,
    display_progress: bool = True,
    **kwargs,
) -> dict[str, GuardList] | None:

    _IGNORED_KWARGS = {
        "config_path",
        "servicex_name",
        "return_exceptions",
        "fail_if_incomplete",
        "progress_bar",
        "concurrency",
        "cache_dir",
    }
    ignored = _IGNORED_KWARGS.intersection(kwargs)
    if ignored:
        logger = logging.getLogger(__name__)
        logger.warning(
            "The following arguments are ignored in servicex-local: %s",
            ", ".join(sorted(ignored)),
        )

    results: dict[str, GuardList] = {}
    cache = _load_cache(adaptor.cache_dir)  # Load cache from file system

    config = _load_ServiceXSpec(spec)

    all_tqs = list(_sample_run_info(config.General, config.Sample))
    total_files = sum(len(tq.file_list or []) for tq in all_tqs)

    with ExpandableProgress(display_progress=display_progress) as progress:
        transform_task = progress.add_task(
            "Transform", start=True, total=total_files
        )

        for tq in all_tqs:
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

        progress.update(
            transform_task,
            "Transform",
            total=total_files,
            completed=total_files,
            refresh=True,
        )
        progress.refresh()

    return results


_deliver_sync = make_sync(deliver_async)
deliver = deprecated(
    reason="Use local_deliver instead. Requires setting up Config.",
)(_deliver_sync)


_DOCKER_IMAGE = "sslhep/servicex_func_adl_xaod_transformer"


def local_deliver(
    spec: Union[ServiceXSpec, Mapping[str, Any], str, Path],
    config: Config,
    display_progress: bool = True,
):
    """Run a query against a dataset, either locally or remotely."""

    logging.basicConfig(level=config.logging_level, force=True)

    if config.platform.value == "singularity":
        image = f"docker://{_DOCKER_IMAGE}:{config.version}"
    else:
        image = f"{_DOCKER_IMAGE}:{config.version}"
    sx_platform = Platform(config.platform.value)
    adaptor = install_sx_local(image, sx_platform)

    sx_result = _deliver_sync(
        spec,
        adaptor=adaptor,
        ignore_local_cache=config.ignore_cache,
        display_progress=display_progress,
    )

    if config.awk:
        awk_result = to_awk(sx_result)
        if len(spec.Sample) == 1:
            return awk_result[spec.Sample[0].Name]
        return awk_result
    return sx_result
