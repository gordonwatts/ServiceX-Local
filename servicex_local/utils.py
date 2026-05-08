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


def find_dataset(
    ds_name: Union[str, list[str]], prefer_local: bool = False
) -> Tuple[
    Union[dataset.FileList, dataset.Rucio, dataset.XRootD, dataset.CERNOpenData], bool
]:
    """Determine the type of dataset based on the input
    string and then return the ServiceX dataset object.
        Also, indicate if it should be accessed locally.

    Args:
        ds_name (str): Name of the dataset to fetch.
        prefer_local (bool): Prefer to run locally if possible.

    Returns:
        Tuple[dataset type, bool]: The dataset object and a flag indicating
        if it should be accessed locally.
    """

    if isinstance(ds_name, list):
        if is_local_filelist(ds_name, True):
            return dataset.FileList(ds_name), True
        if is_local_filelist(ds_name, False):
            return dataset.FileList(ds_name), prefer_local
        return dataset.FileList(ds_name), False

    if is_stored_locally(ds_name):
        return dataset.FileList([ds_name]), True

    ds = ds_type_resolver(ds_name)

    if isinstance(ds, dataset.Rucio):
        return ds, False
    if isinstance(ds, dataset.XRootD):
        return ds, False
    if isinstance(ds, dataset.CERNOpenData):
        return ds, prefer_local
    if isinstance(ds, dataset.FileList):
        if re.match(r"^https?://", ds_name):
            # Special case for CERNBox URLs
            if not prefer_local:
                parsed_url = urlparse(ds_name)
                if (
                    "cernbox.cern.ch" in parsed_url.netloc
                    and parsed_url.path.startswith("/files/spaces")
                ):
                    return ds, False
            return ds, prefer_local
        if re.match(r"^root://", ds_name):
            return ds, False


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

    # TODO: Add a test to make sure this returns the right directory.
    # TODO: Add test for what happens with no servicex.yaml
    try:
        sx_cfg = Configuration.read()
        cache_dir = Path(sx_cfg.cache_path).resolve()
    except NameError:
        import tempfile
        
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_dir = Path(temp_dir).resolve()
            logging.warning(f"Could not read a ServiceX.yaml. Using temporary directory for cache.")
            

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
