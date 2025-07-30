import logging
import os
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional

from .logging_decorator import log_to_file

def run_command_with_logging(command: List[str], log_file: Path) -> None:
    """Run a command in a subprocess and log the output.

    Args:
        command (List[str]): The command to run
        log_file (Path): The file to write log messages to

    Raises:
        RuntimeError: If the command fails
    """

    @log_to_file(log_file)
    def do_the_work():
        logger = logging.getLogger(__name__)
        logger.debug("Running command: %s", " ".join(command))
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        stdout_lines = []

        assert process.stdout is not None

        emit_next_line_level: Optional[int] = None
        for stdout_line in iter(process.stdout.readline, ""):
            stripped_line = stdout_line.strip()
            stdout_lines.append(stripped_line)
            emitted_level: Optional[int] = None
            if (emit_next_line_level == logging.ERROR) or (
                "error" in stripped_line.lower()
            ):
                logger.error(stripped_line)
                emitted_level = logging.ERROR
            elif (emit_next_line_level == logging.WARNING) or (
                "warning" in stripped_line.lower()
            ):
                logger.warning(stripped_line)
                emitted_level = logging.WARNING
            else:
                logger.debug(stripped_line)

            emit_next_line_level = None
            if stripped_line.endswith(":"):
                emit_next_line_level = emitted_level

        process.stdout.close()
        return_code = process.wait()

        if return_code != 0:
            # Output the log as info
            for line in stdout_lines:
                logger.info(line)

            # TODO: Once we are done with 3.11, get rid of newline. Problem is
            #       we can't have a \n in an f-string for the older versions of python.
            raise RuntimeError(
                f"Failed to run SX science payload locally with exit_code={return_code} "
                f"({' '.join(command)}). See INFO python logging messages for more details"
            )

    do_the_work()


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
        assert (
            generated_files_dir.exists()
        ), f"Missing generate files directory: {generated_files_dir}!"
        wsl_generated_files_dir = self._convert_to_wsl_path(generated_files_dir)

        for input_file in input_files:
            # Check if input_file is a root:// or http:// path
            if (
                input_file.startswith("root://")
                or input_file.startswith("http://")
                or input_file.startswith("https://")
            ):
                wsl_input_file = input_file
                input_path_name = Path(input_file.split("/")[-1]).name
            else:
                # Translate input_file to WSL2 path
                input_path = Path(input_file)
                assert input_path.exists(), f"Missing input file: {input_file}"
                wsl_input_file = self._convert_to_wsl_path(input_path)
                input_path_name = input_path.name

            # Create the script to parse the capabilities file.
            file_runner = f"""#!/bin/python
import json
import os
import sys

x509up_path = "/tmp/grid-security/x509up"
if os.path.exists(x509up_path):
    os.chmod(x509up_path, 0o600)
    os.system("ls -l " + x509up_path)

with open("{wsl_generated_files_dir}/transformer_capabilities.json") as f:
    info = json.load(f)
file_to_run = info["command"]
# Strip off the default /generated from the file name.
file_to_run = file_to_run.replace("/generated", "")
if info["language"] == "python":
    ret_code = os.system("python3 {wsl_generated_files_dir}/" + file_to_run + " {wsl_input_file} "
        + "{wsl_output_directory}/{input_path_name} {output_format}")
elif info["language"] == "bash":
    ret_code = os.system("bash {wsl_generated_files_dir}/" + file_to_run
        + " {wsl_input_file} {wsl_output_directory}/{input_path_name} {output_format}")
else:
    raise ValueError("Unsupported language: " + info["language"])

exit_code = ret_code >> 8
sys.exit(exit_code)
"""
            with open(generated_files_dir / "kick_off.py", "w", newline="\n") as f:
                f.write(file_runner)

            # Create the WSL script content
            wsl_script_content = f"""#!/bin/bash
tmp_dir=$(mktemp -d -t ci-XXXXXXXXXX)
cd $tmp_dir
pwd

# source /etc/profile.d/startup-atlas.sh
setupATLAS
asetup AnalysisBase,{self._release},here
python {wsl_generated_files_dir}/kick_off.py
r=$?
exit $r
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
            command = ["wsl", "-d", self._container, "bash", "-i", wsl_script_path]
            run_command_with_logging(
                command, log_file=generated_files_dir / "wsl_log.txt"
            )
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
        x509up_path = Path(os.getenv("TEMP", "/tmp")) / "x509up"
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

            # Create docker mapping string for the input file if it exists.
            if (
                input_file.startswith("root://")
                or input_file.startswith("http://")
                or input_file.startswith("https://")
            ):
                input_volume = []
                container_path = input_file
            else:
                input_path = Path(input_file)
                if not input_path.exists():
                    raise FileNotFoundError(
                        f"Input file for docker science image {input_file}"
                        " not found."
                    )
                input_volume = ["-v", f"{str(input_path.absolute())}:/input_file.root"]
                container_path = "/input_file.root"

            # Create the file that will actually do the work. We need to look at the transformer
            # capabilities json file to figure it out.
            file_runner = """#!/bin/bash
python_cmd=$(command -v python3 || command -v python)
exec $python_cmd /generated/kick_off.py $@
"""
            with open(generated_files_dir / "file_runner.sh", "w", newline="\n") as f:
                for ln in file_runner.splitlines():
                    f.write(ln.strip() + "\n")

            # Create the file that will actually do the work. We need to look at the transformer
            # capabilities json file to figure it out.
            kick_off = """
import json
import os
import sys

x509up_path = "/tmp/grid-security/x509up"
if os.path.exists(x509up_path):
    os.chmod(x509up_path, 0o600)

with open("/generated/transformer_capabilities.json") as f:
    info = json.load(f)
file_to_run = info["command"]
arg1 = sys.argv[1]
arg2 = sys.argv[2]
arg3 = sys.argv[3]
if info["language"] == "python":
    exe = sys.executable
    ret_code = os.system(exe + " " + file_to_run + " " + arg1 + " " + arg2 + " " + arg3)
elif info["language"] == "bash":
    ret_code = os.system("bash " + file_to_run + " " + arg1 + " " + arg2 + " " + arg3)
else:
    raise ValueError("Unsupported language: " + info["language"])
exit_code = ret_code >> 8
sys.exit(exit_code)
"""
            with open(generated_files_dir / "kick_off.py", "w") as f:
                f.write(kick_off)

            try:
                command = [
                    "docker",
                    "run",
                    "--name",
                    container_name,
                    "--rm",
                    "-v",
                    f"{generated_files_dir.absolute()}:/generated",
                    "-v",
                    f"{output_directory}:/servicex/output",
                    *x509up_volume,
                    *input_volume,
                    self.image_name,
                    "bash",
                    "/generated/file_runner.sh",
                    container_path,
                    f"/servicex/output/{output_name}",
                    output_format,
                ]
                run_command_with_logging(
                    command, log_file=generated_files_dir / "docker_log.txt"
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


class SingularityScienceImage(BaseScienceImage):
    def __init__(self, image_uri: str):
        """Science image will run in a Singularity container with the specified image URI

        Args:
            image_uri (str): The path/URI of the Singularity image
        """
        self.image_uri = image_uri

    def transform(
        self,
        generated_files_dir: Path,
        input_files: List[str],
        output_directory: Path,
        output_format: str,
    ) -> List[Path]:
        """Transform the input files and return the path to the output file.

        This method works by invoking the container with a specific transformation command.

        Args:
            generated_files_dir (str): Directory for generated files
            input_files (List[str]): List of input files
            output_directory (Path): The output directory
            output_format (str): The desired output format

        Returns:
            List[Path]: List of output file paths
        """

        output_paths = []
        x509up_path = Path(os.getenv("TEMP", "/tmp")) / "x509up"
        if x509up_path.exists():
            x509up_volume = ["--bind", f"{x509up_path}:/tmp/grid-security/x509up"]
        else:
            logger = logging.getLogger(__name__)
            logger.warning("x509up certificate not found at /tmp/x509up")
            x509up_volume = []

        for input_file in input_files:
            safe_image = self.image_uri.replace(":", "_").replace("/", "_")
            container_name = f"sx_codegen_container_{safe_image}_{Path(input_file).stem}"

            output_name = Path(input_file).name

            if input_file.startswith(("root://", "http://", "https://")):
                input_volume = []
                container_path = input_file
            else:
                input_path = Path(input_file)
                if not input_path.exists():
                    raise FileNotFoundError(
                        f"Input file for Singularity image {input_file} not found."
                    )
                input_volume = ["--bind", f"{str(input_path.absolute())}:/input_file.root"]
                container_path = "/input_file.root"

            file_runner = """#!/bin/bash
python_cmd=$(command -v python3 || command -v python)
exec $python_cmd /generated/kick_off.py $@
"""
            with open(generated_files_dir / "file_runner.sh", "w", newline="\n") as f:
                for ln in file_runner.splitlines():
                    f.write(ln.strip() + "\n")

            kick_off = """
import json
import os
import sys

x509up_path = "/tmp/grid-security/x509up"
if os.path.exists(x509up_path):
    os.chmod(x509up_path, 0o600)

with open("/generated/transformer_capabilities.json") as f:
    info = json.load(f)
file_to_run = info["command"]
arg1 = sys.argv[1]
arg2 = sys.argv[2]
arg3 = sys.argv[3]
if info["language"] == "python":
    exe = sys.executable
    ret_code = os.system(exe + " " + file_to_run + " " + arg1 + " " + arg2 + " " + arg3)
elif info["language"] == "bash":
    ret_code = os.system("bash " + file_to_run + " " + arg1 + " " + arg2 + " " + arg3)
else:
    raise ValueError("Unsupported language: " + info["language"])
exit_code = ret_code >> 8
sys.exit(exit_code)
"""
            with open(generated_files_dir / "kick_off.py", "w") as f:
                f.write(kick_off)

            import tempfile

            with tempfile.TemporaryDirectory() as temp_dir:

                print(f"Temporary directory created at: {temp_dir}")

                try:
                    command = [
                        "singularity",
                        "exec",
                        *x509up_volume,
                        *input_volume,
                        "--bind", f"{generated_files_dir.absolute()}:/generated",
                        "--bind", f"{output_directory}:/servicex/output",
                        "--pwd", str(temp_dir),
                        self.image_uri,
                        "bash",
                        "/generated/file_runner.sh",
                        container_path,
                        f"/servicex/output/{output_name}",
                        output_format,
                    ]
                    run_command_with_logging(command, log_file=generated_files_dir / "singularity_log.txt")
                    output_paths.append(output_directory / Path(input_file).name)

                except subprocess.CalledProcessError as e:
                    raise RuntimeError(
                        f"Failed to start Singularity container for {input_file}: "
                        f"{e.stderr.decode('utf-8')}"
                    )

        output_files = list(output_directory.glob("*"))
        if len(output_files) != len(input_files):
            raise RuntimeError(
                f"Number of output files ({len(output_files)}) does not match number of "
                f"input files ({len(input_files)})"
            )

        return output_files