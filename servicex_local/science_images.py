from abc import ABC, abstractmethod
from pathlib import Path
import subprocess
from typing import List


class BaseScienceImage(ABC):
    @abstractmethod
    def transform(
        self,
        generated_files_dir: Path,
        input_files: List[str],
        output_directory: Path,
        output_format: str,
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files
            output_directory (Path): The output directory
            output_format (str): The desired output format

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
        self,
        generated_files_dir: Path,
        input_files: List[str],
        output_directory: Path,
        output_format: str,
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files
            output_directory (Path): The output directory
            output_format (str): The desired output format

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
        self.image_name = image_name

    def transform(
        self,
        generated_files_dir: Path,
        input_files: List[str],
        output_directory: Path,
        output_format: str,
    ) -> List[Path]:
        """Transform the input directory and return the path to the output file.

        Science images are basically one-trick-pony's - they have an command line
        api. That makes running them very simple.

        This runs in synchronous mode - the call will not return.

        Args:
            generated_files_dir (str): The input directory
            input_files (List[str]): List of input files
            output_directory (Path): The output directory
            output_format (str): The desired output format

        Returns:
            List[Path]: The paths to the output files
        """
        output_paths = []
        for input_file in input_files:
            safe_image = self.image_name.replace(":", "_").replace("/", "_")
            container_name = (
                f"sx_codegen_container_{safe_image}_{Path(input_file).stem}"
            )

            output_name = Path(input_file).name

            try:
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "--name",
                        container_name,
                        "--rm",
                        "-v",
                        f"{generated_files_dir}:/generated",
                        "-v",
                        f"{output_directory}:/servicex/output",
                        self.image_name,
                        "python",
                        "/generated/transform_single_file.py",
                        input_file,
                        f"/servicex/output/{output_name}",
                        output_format,
                    ],
                    check=True,
                    stderr=subprocess.PIPE,
                )
                output_paths.append(output_directory / Path(input_file).name)

            except subprocess.CalledProcessError as e:
                raise RuntimeError(
                    f"Failed to start docker container for {input_file}: "
                    f"{e.stderr.decode('utf-8')}"
                )

        output_files = list(output_directory.glob("*"))
        if len(output_files) != len(input_files):
            raise RuntimeError(
                f"Number of output files ({len(output_files)}) does not match number of "
                f"input files ({len(input_files)})"
            )

        return output_files
