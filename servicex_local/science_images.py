import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
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

    def _convert_to_wsl_path(self, path: Path) -> str:
        """Convert a Windows path to a WSL path

        Args:
            path (Path): The Windows path

        Returns:
            str: The WSL path
        """
        return (
            f"/mnt/{path.absolute().drive[0].lower()}{path.absolute().as_posix()[2:]}"
        )

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
        output_paths = []

        # Translate output_directory to WSL2 path
        if not output_directory.exists():
            output_directory.mkdir(parents=True, exist_ok=True)
        wsl_output_directory = self._convert_to_wsl_path(output_directory)

        # Translate generated files dir to WSL2 path
        wsl_generated_files_dir = self._convert_to_wsl_path(generated_files_dir)

        for input_file in input_files:
            # Check if input_file is a root:// or http:// path
            if input_file.startswith("root://") or input_file.startswith("http://"):
                wsl_input_file = input_file
                input_path_name = Path(input_file.split("/")[-1]).name
            else:
                # Translate input_file to WSL2 path
                input_path = Path(input_file)
                wsl_input_file = self._convert_to_wsl_path(input_path)
                input_path_name = input_path.name

            # Create the WSL script content
            wsl_script_content = f"""#!/bin/bash
tmp_dir=$(mktemp -d -t ci-XXXXXXXXXX)
cd $tmp_dir

# source /etc/profile.d/startup-atlas.sh
setupATLAS
asetup AnalysisBase,{self._release},here
source {wsl_generated_files_dir}/transform_single_file.sh {wsl_input_file} {wsl_output_directory}/{input_path_name}  # noqa
"""

            # Write the script to a temporary file
            script_path = generated_files_dir / "wsl_transform_script.sh"
            with open(script_path, "w", newline="\n") as script_file:
                script_file.write(wsl_script_content)
            # Convert script_path to a WSL accessible path
            wsl_script_path = self._convert_to_wsl_path(script_path)

            # Make the script executable
            os.chmod(script_path, 0o755)

            # Call the WSL command via os.system
            command = f"wsl -d {self._container} bash -i {wsl_script_path}"
            r = os.system(command)
            if r != 0:
                raise RuntimeError(f"Failed to run command: {r} - {command}")
            output_paths.append(output_directory / input_path_name)

        return output_paths


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
        x509up_path = Path("/tmp/x509up")
        if x509up_path.exists():
            x509up_volume = ["-v", f"{x509up_path}:/tmp/grid-security/x509up"]
        else:
            logger = logging.getLogger(__name__)
            logger.warning("x509up certificate not found at /tmp/x509up")
            x509up_volume = []

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
                        *x509up_volume,
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
