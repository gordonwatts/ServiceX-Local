from abc import ABC, abstractmethod
from io import BytesIO
from zipfile import ZipFile
from requests_toolbelt.multipart import decoder
from pathlib import Path
import subprocess

import requests
from tenacity import retry, stop_after_attempt, wait_fixed


class SXCodeGen(ABC):
    def __init__(self):
        """Abstract base class for code-generators."""
        pass

    @abstractmethod
    def gen_code(self, query: str, directory: Path) -> Path:
        """Generate code based on the query and save all files to the
        requested directory.

        Args:
            query: The query string to generate code for.
            directory: The path to the directory where the code should be saved.

        Returns:
            Path: The path to the directory where the code was saved.
        """
        pass


class LocalXAODCodegen(SXCodeGen):
    def __init__(self):
        """Create a code-generator for func_adl running on xAOD.

        Local code (e.g. the func_adl_xAOD package must be installed) is used.
        """
        pass

    def gen_code(self, query: str, directory: Path) -> Path:
        raise NotImplementedError("gen_code method is not implemented for LocalXAOD")


class DockerCodegen(SXCodeGen):
    def __init__(self, image_name: str):
        """Create a code-generator that uses a pre-made SX
        code generator docker image.

        Args:
            image_name: The name of the docker image to run.
        """
        self.image_name = image_name

    def gen_code(self, query: str, directory: Path) -> Path:
        """Run the code generator docker image, and save the results
        to the given directory.

        NOTE: The directory should be empty when this is first called,
        though that isn't checked!

        Args:
            query (str): The `quastle` (or other format) query.
            directory (Path): Where the output files should be written.

        Returns:
            Path: compressed file containing all the code required.
        """
        safe_image = self.image_name.replace(":", "_").replace("/", "_")
        container_name = f"sx_codegen_container_{safe_image}"

        try:
            # Run the docker container in the background
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--name",
                    container_name,
                    "-d",  # Run in detached mode
                    "-v",
                    f"{directory}:/output",
                    "-p",
                    "5000:5000",
                    self.image_name,
                ],
                check=True,
                stderr=subprocess.PIPE,
            )

            # Next, run the query against the code generator.
            @retry(stop=stop_after_attempt(10), wait=wait_fixed(1))
            def run_query(query: str):
                post_url = f"http://localhost:{5000}"
                postObj = {"code": query}
                r = requests.post(post_url + "/servicex/generated-code", json=postObj)
                r.raise_for_status()
                return r

            result = run_query(query)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Failed to start docker container: {e.stderr.decode('utf-8')}"
            )
        finally:
            # Ensure the container is stopped and removed
            subprocess.run(["docker", "rm", "-f", container_name], check=False)

        # Unpack the files into a zip file and store them.
        decoder_parts = decoder.MultipartDecoder.from_response(result)
        zipfile_content = decoder_parts.parts[3].content
        zipfile = ZipFile(BytesIO(zipfile_content))

        if not directory.exists():
            directory.mkdir(parents=True)
        zipfile.extractall(directory)

        # The request should come back as a zip file. We now unpack that.
        return directory
