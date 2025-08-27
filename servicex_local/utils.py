import logging
import os
import re
from enum import Enum
from pathlib import Path
from typing import Tuple, Union
from urllib.parse import unquote, urlparse

from servicex import dataset


class Platform(Enum):
    """Options for which platform to use for the runtime environment."""

    docker = "docker"
    singularity = "singularity"
    wsl2 = "wsl2"


def find_dataset(
    ds_name: str, prefer_local: bool = False
) -> Tuple[Union[dataset.FileList, dataset.Rucio, dataset.XRootD], bool]:
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
    what_is_it = None

    if re.match(r"^https?://", ds_name):
        what_is_it = "url"
        url = ds_name

        # Special case for CERNBox URLs
        if not prefer_local:
            parsed_url = urlparse(url)
            if "cernbox.cern.ch" in parsed_url.netloc and parsed_url.path.startswith(
                "/files/spaces"
            ):
                remote_file = f"root://eospublic.cern.ch{parsed_url.path[13:]}"
                what_is_it = "remote_file"

    elif re.match(r"^file://", ds_name):
        parsed_uri = urlparse(ds_name)
        file_path = unquote(parsed_uri.path)
        if os.name == "nt" and file_path.startswith("/"):
            file_path = file_path[1:]
        file = Path(file_path).absolute()
        what_is_it = "file"

    elif re.match(r"^rucio://", ds_name):
        what_is_it = "rucio"
        did = ds_name[8:]

    else:
        file = Path(ds_name).absolute()
        if file.exists():
            what_is_it = "file"
        else:
            if os.path.sep in ds_name:
                raise ValueError(
                    f"{ds_name} looks like a file path, but the file does not exist"
                )
            did = ds_name
            what_is_it = "rucio"

    if what_is_it == "url":
        logging.debug("Interpreting %s as a URL", ds_name)
        return dataset.FileList([url]), prefer_local

    if what_is_it == "file":
        logging.debug("Interpreting %s as a local file (%s)", ds_name, file)
        if file.exists():
            return dataset.FileList([str(file)]), True
        raise ValueError(f"Local file {file} does not exist.")

    if what_is_it == "remote_file":
        logging.debug("Interpreting %s as a remote file (%s)", ds_name, remote_file)
        return dataset.FileList([remote_file]), False

    if what_is_it == "rucio":
        logging.debug("Interpreting %s as a Rucio dataset (%s)", ds_name, did)
        return dataset.Rucio(did), False

    raise RuntimeError(f"Unknown input type: {what_is_it}")


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
    from servicex_local import LocalXAODCodegen, SXLocalAdaptor

    codegen_name = "local"
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
        codegen, science_runner, codegen_name, f"http://localhost:{host_port}"
    )

    logging.info("Using local ServiceX endpoint: codegen %s", codegen_name)
    return codegen_name, adaptor
