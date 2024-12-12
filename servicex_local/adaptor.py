from typing import List, Optional, Dict
from servicex.models import Status, TransformRequest, TransformStatus, CachedDataset
from servicex_local.codegen import SXCodeGen
from servicex_local.science_images import BaseScienceImage
from pathlib import Path
import tempfile
import uuid


class SXLocalAdaptor:

    def __init__(
        self, codegen: SXCodeGen, science_runner: BaseScienceImage, codegen_name: str
    ):
        self.codegen = codegen
        self.science_runner = science_runner
        self.codegen_name = codegen_name
        self.transform_status_store: Dict[str, TransformStatus] = {}

    async def get_transforms(self) -> List[TransformStatus]:
        # Implement local logic to get transforms
        # For example, read from a local file or database
        return []

    def get_code_generators(self) -> List[str]:
        # Return the code generator name provided during initialization
        return [self.codegen_name]

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

    async def submit_transform(self, transform_request: TransformRequest) -> str:
        # Generate code using the code generator
        with tempfile.TemporaryDirectory() as generated_files_dir:
            generated_files_dir = Path(generated_files_dir)
            self.codegen.gen_code(transform_request.selection, generated_files_dir)

            # Run the science image to perform the transformation
            input_files = transform_request.file_list
            assert input_files is not None, "Local transform needs an actual file list"
            output_directory = generated_files_dir / "output"
            output_directory.mkdir(parents=True, exist_ok=True)
            output_format = transform_request.result_format.name

            output_files = self.science_runner.transform(
                generated_files_dir, input_files, output_directory, output_format
            )

            # Store the TransformStatus indexed by a GUID
            request_id = str(uuid.uuid4())
            transform_status = TransformStatus(
                did=",".join(input_files),
                selection=transform_request.selection,
                request_id=request_id,
                status=Status.complete,
                tree_name="",
                image="",
                result_destination=transform_request.result_destination,
                result_format=transform_request.result_format,
                files_completed=len(output_files),
                files_failed=0,
                files_remaining=0,
                files=len(input_files),
                app_version="",
                generated_code_cm="",
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
