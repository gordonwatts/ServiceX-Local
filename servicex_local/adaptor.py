from datetime import datetime
from typing import List, Optional, Dict
from servicex.models import (
    Status,
    TransformRequest,
    TransformStatus,
    CachedDataset,
    ResultFile,
)
from servicex_local.codegen import SXCodeGen
from servicex_local.science_images import BaseScienceImage
from pathlib import Path
import tempfile
import uuid


class SXLocalAdaptor:

    def __init__(
        self,
        codegen: SXCodeGen,
        science_runner: BaseScienceImage,
        codegen_name: str,
        url: str,
    ):
        self.codegen = codegen
        self.science_runner = science_runner
        self.codegen_name = codegen_name
        self.url = url
        self.transform_status_store: Dict[str, TransformStatus] = {}

    async def _get_authorization(self):
        "Dummied out - we always have authorization"
        # TODO: Why is this method start with a `_`?
        pass

    async def get_transforms(self) -> List[TransformStatus]:
        # Implement local logic to get transforms
        # For example, read from a local file or database
        raise NotImplementedError(
            "get_transforms is not implemented for SXLocalAdaptor"
        )

    def get_code_generators(self) -> Dict[str, List[str]]:
        # Return the code generator name provided during initialization
        return {self.codegen_name: []}

    async def get_datasets(
        self, did_finder: Optional[str] = None, show_deleted: bool = False
    ) -> List[CachedDataset]:
        raise NotImplementedError("get_datasets is not implemented for SXLocalAdaptor")

    async def get_dataset(self, dataset_id: Optional[str] = None) -> CachedDataset:
        raise NotImplementedError("get_dataset is not implemented for SXLocalAdaptor")

    async def delete_dataset(self, dataset_id: Optional[str] = None) -> bool:
        raise NotImplementedError(
            "delete_dataset is not implemented for SXLocalAdaptor"
        )

    def create_transform_status(
        self,
        transform_request: TransformRequest,
        request_id: str,
        output_files: List[Path],
    ) -> TransformStatus:
        assert transform_request.file_list is not None, "File list is required"
        return TransformStatus(
            **{
                "did": ",".join(transform_request.file_list),
                "selection": transform_request.selection,
                "request_id": request_id,
                "status": Status.complete,
                "tree-name": "mytree",
                "image": "doit",
                "result-destination": transform_request.result_destination,
                "result-format": transform_request.result_format,
                "files-completed": len(output_files),
                "files-failed": 0,
                "files-remaining": 0,
                "files": len(transform_request.file_list),
                "app-version": "this",
                "generated-code-cm": "this",
                "submit-time": datetime.now(),
            }
        )

    async def submit_transform(self, transform_request: TransformRequest) -> str:
        """
        Submits a transformation request and processes the transformation.

        Args:
            transform_request (TransformRequest): The transformation request containing
                the selection, file list, result format, and result destination.

        Returns:
            str: A unique request ID for the transformation.

        Raises:
            AssertionError: If the file list in the transform_request is None.

        This method performs the following steps:
        1. Creates a temporary directory for generated files.
        2. Generates code based on the selection in the transform request.
        3. Creates a unique directory for the output files.
        4. Runs the science image to perform the transformation on the input files.
        5. Stores the transformation status indexed by a GUID.
        6. Returns the GUID as the request ID.
        """
        with tempfile.TemporaryDirectory() as generated_files_dir:
            generated_files_dir = Path(generated_files_dir)
            self.codegen.gen_code(transform_request.selection, generated_files_dir)

            # Create a unique directory for the output files directly under the temp directory
            request_id = str(uuid.uuid4())
            output_directory = Path(tempfile.gettempdir()) / f"servicex/{request_id}"
            output_directory.mkdir(parents=True, exist_ok=True)

            # Run the science image to perform the transformation
            input_files = transform_request.file_list
            assert input_files is not None, "Local transform needs an actual file list"
            output_format = transform_request.result_format.name

            output_files = self.science_runner.transform(
                generated_files_dir, input_files, output_directory, output_format
            )

            # Store the TransformStatus indexed by a GUID
            transform_status = self.create_transform_status(
                transform_request, request_id, output_files
            )
            self.transform_status_store[request_id] = transform_status

            # Return the GUID as the request ID
            return request_id

    async def get_transform_status(self, request_id: str) -> TransformStatus:
        # Retrieve the TransformStatus from the store using the request ID
        transform_status = self.transform_status_store.get(request_id)
        if not transform_status:
            raise ValueError(f"No transform found for request ID {request_id}")

        return transform_status


class MinioLocalAdaptor:
    def __init__(self, bucket: str, **kwargs):
        self.request_id = bucket

    @classmethod
    def for_transform(cls, transform: TransformStatus):
        return cls(
            endpoint_host=transform.minio_endpoint,  # type: ignore
            secure=transform.minio_secured,  # type: ignore
            access_key=transform.minio_access_key,  # type: ignore
            secret_key=transform.minio_secret_key,  # type: ignore
            bucket=transform.request_id,
        )

    async def list_bucket(self) -> List[ResultFile]:
        output_directory = Path(tempfile.gettempdir()) / f"servicex/{self.request_id}"
        result_files = []
        for file_path in output_directory.glob("*"):
            if file_path.is_file():
                result_files.append(
                    ResultFile(
                        filename=file_path.name,
                        size=file_path.stat().st_size,
                        extension=file_path.suffix[1:],  # Remove the leading dot
                    )
                )
        return result_files

    async def download_file(
        self, object_name: str, local_dir: str, shorten_filename: bool = False
    ) -> Path:
        output_directory = Path(tempfile.gettempdir()) / f"servicex/{self.request_id}"
        source_path = output_directory / object_name
        destination_path = Path(local_dir) / object_name

        if not source_path.exists():
            raise FileNotFoundError(
                f"File {object_name} not found in {output_directory}"
            )

        destination_path.parent.mkdir(parents=True, exist_ok=True)
        destination_path.write_bytes(source_path.read_bytes())

        return destination_path.resolve()

    async def get_signed_url(self, object_name: str) -> str:
        output_directory = Path(tempfile.gettempdir()) / f"servicex/{self.request_id}"
        file_path = output_directory / object_name

        if not file_path.exists():
            raise FileNotFoundError(
                f"File {object_name} not found in {output_directory}"
            )

        return file_path.resolve().as_uri()

    @classmethod
    def hash_path(cls, file_name):
        raise NotImplementedError("hash_path is not implemented for MockMinioAdapter")
