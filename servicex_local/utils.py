import logging
import os
import re
from enum import Enum
from pathlib import Path
from typing import Tuple, Union
from urllib.parse import unquote, urlparse

from servicex import dataset
from servicex_analysis_utils import ds_type_resolver


class Platform(Enum):
    """Options for which platform to use for the runtime environment."""

    docker = "docker"
    singularity = "singularity"
    wsl2 = "wsl2"


def is_local_filelist(file_list: list[str], force_local: bool) -> bool:
    """Check if the provided list of dataset names is valid.
    Args:
        file_list (list[str]): List of dataset names.
        force_local (bool): Force local access. Used to
        check if any of the files require local access.
    Returns:
        bool: True if the list is valid, False otherwise.
    """
    for ds in file_list:
        if find_dataset(ds, not force_local)[1] is force_local:
            return force_local
    return not force_local


def is_stored_locally(ds_name: str) -> bool:
    """Check if the provided dataset name is a local file path.
    Args:
        ds_name (str): Dataset name.
    Returns:
        bool: True if the dataset is a local file path, False otherwise.
    """
    file = Path(ds_name).absolute()
    if file.exists():
        return True
    if re.match(r"^file://", ds_name):
        parsed_uri = urlparse(ds_name)
        file_path = unquote(parsed_uri.path)
        if os.name == "nt" and file_path.startswith("/"):
            file_path = file_path[1:]
        file = Path(file_path).absolute()
        return True
    return False


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
    from pathlib import Path

    from servicex_local import LocalXAODCodegen, SXLocalAdaptor
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
        from servicex_local import DockerScienceImage

        science_runner = DockerScienceImage(image)

    elif platform == Platform.singularity:
        from servicex_local import SingularityScienceImage

        science_runner = SingularityScienceImage(image)

    elif platform == Platform.wsl2:
        from servicex_local import WSL2ScienceImage

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
