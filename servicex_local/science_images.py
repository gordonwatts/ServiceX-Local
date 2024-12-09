from abc import ABC, abstractmethod
from pathlib import Path
from typing import List


class BaseScienceImage(ABC):
    @abstractmethod
    def transform(
        self, generated_files_dir: Path, input_files: List[str]
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files

        Returns:
            List[Path]: The paths to the output files
        """
        pass


class WSL2ScienceImage(BaseScienceImage):
    def __init__(self, wsl2_container: str, atlas_release: str):
        """Science image will run in a WSL2 container with the specified ATLAS release

        Args:
            wsl2_container (str): Which WSL2 container should be used ("al9_atlas")
            atlas_release (str): Which release should be used ("22.2.107")
        """
        self._release = atlas_release
        self._container = wsl2_container

    def transform(
        self, generated_files_dir: Path, input_files: List[str]
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files

        Returns:
            List[Path]: The paths to the output files
        """
        # Implement the transformation logic here
        raise NotImplementedError("This WSL2 Sci Image is not implemented yet")


class DockerScienceImage(BaseScienceImage):
    def __init__(self, image_name: str):
        """Science image will run in a Docker container with the specified image name/tag

        Args:
            image_name (str): The name/tag of the Docker image to be used
        """
        self._image_name = image_name

    def transform(
        self, generated_files_dir: Path, input_files: List[str]
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files

        Returns:
            List[Path]: The paths to the output files
        """
        # Implement the transformation logic here
        raise NotImplementedError("This Docker Sci Image is not implemented yet")
