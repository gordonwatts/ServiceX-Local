import importlib
import json
import logging
import urllib.request
from dataclasses import dataclass
from typing import TYPE_CHECKING, Union
from .utils import Platform


if TYPE_CHECKING:
    from func_adl_servicex_xaodr21.sx_dataset import FuncADLQueryPHYS as _PHYS21
    from func_adl_servicex_xaodr22.sx_dataset import FuncADLQueryPHYS as _PHYS22
    from func_adl_servicex_xaodr22.sx_dataset import FuncADLQueryPHYSLITE as _PHYSLITE22
    from func_adl_servicex_xaodr25.sx_dataset import FuncADLQueryPHYS as _PHYS25
    from func_adl_servicex_xaodr25.sx_dataset import FuncADLQueryPHYSLITE as _PHYSLITE25

_DOCKER_IMAGE = "sslhep/servicex_func_adl_xaod_transformer"


_VALID_RELEASES = (21, 22, 25)


@dataclass
class Config:
    """Base configuration for ServiceX local datasets."""

    version: str = "latest"
    platform: Union[Platform, str] = "docker"
    ignore_cache: bool = False
    awk: bool = False
    logging_level: str = "WARNING"

    def __post_init__(self):
        if isinstance(self.platform, str):
            self.platform = Platform[self.platform]
        if self.logging_level not in logging._nameToLevel:
            valid = sorted(logging._nameToLevel)
            raise ValueError(
                f"logging_level must be one of {valid}, got {self.logging_level!r}"
            )


@dataclass
class xAODConfig(Config):
    """Configuration for xAOD datasets."""

    release: int = 21

    def __post_init__(self):
        super().__post_init__()
        if self.release not in _VALID_RELEASES:
            raise ValueError(f"release must be one of {_VALID_RELEASES}, got {self.release}")
        if self.version == "latest":
            self.version = self._latest_for_release(self.release)

    def _latest_for_release(self, release: int) -> str:
        versions = [t for t in self.available_versions if t.startswith(f"{release}.")]
        if not versions:
            raise ValueError(f"No versions found for release {release}")
        return max(versions, key=lambda t: tuple(int(x) for x in t.split(".")))

    @property
    def available_versions(self) -> list[str]:
        """Return available versions (21.x, 22.x, 25.x) of the xAOD transformer Docker image."""
        tags = []
        url = f"https://hub.docker.com/v2/repositories/{_DOCKER_IMAGE}/tags?page_size=100"
        while url:
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read())
            tags.extend(result["name"] for result in data["results"])
            url = data.get("next")
        return [t for t in tags if t.startswith(("21.", "22.", "25."))]

    @property
    def latest_r21_version(self) -> str:
        """Return the latest 21.x version of the xAOD transformer Docker image."""
        return self._latest_for_release(21)

    @property
    def latest_r22_version(self) -> str:
        """Return the latest 22.x version of the xAOD transformer Docker image."""
        return self._latest_for_release(22)

    @property
    def latest_r25_version(self) -> str:
        """Return the latest 25.x version of the xAOD transformer Docker image."""
        return self._latest_for_release(25)

    def _release_module(self):
        return importlib.import_module(f"func_adl_servicex_xaodr{self.release}")

    def FuncADLQueryPHYS(self) -> "Union[_PHYS21, _PHYS22, _PHYS25]":
        return self._release_module().FuncADLQueryPHYS()

    def FuncADLQueryPHYSLITE(self) -> "Union[_PHYSLITE22, _PHYSLITE25]":
        return self._release_module().FuncADLQueryPHYSLITE()