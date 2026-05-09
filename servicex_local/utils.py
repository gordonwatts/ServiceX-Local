import logging
from enum import Enum
from pathlib import Path


class Platform(Enum):
    """Options for which platform to use for the runtime environment."""

    docker = "docker"
    singularity = "singularity"
    wsl2 = "wsl2"


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
