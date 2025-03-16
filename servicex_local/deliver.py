from typing import Any, Generator, List

from make_it_sync import make_sync
from servicex import General, ResultDestination, Sample, ServiceXSpec
from servicex.models import ResultFormat, TransformRequest
from servicex.servicex_client import GuardList
from servicex_local import SXLocalAdaptor
from servicex_local.adaptor import MinioLocalAdaptor


def _sample_run_info(
    g: General, samples: List[Sample]
) -> Generator[TransformRequest, Any, None]:
    """
    Generate TransformRequest objects for a list of samples.

    Args:
        g (General): A general configuration object.
        samples (List[Sample]): A list of Sample objects containing information about each sample.
    Yields:
        TransformRequest: A TransformRequest object for each sample in the list.
    """
    for s in samples:

        tq = TransformRequest(
            title=s.Name,
            codegen="local-codegen",
            selection=str(s.Query),
            result_destination=ResultDestination.object_store,
            result_format=ResultFormat.root_ttree,
        )

        yield tq


async def deliver_async(
    spec: ServiceXSpec,
    adaptor: SXLocalAdaptor,
    **kwargs,
) -> dict[str, GuardList] | None:

    results: dict[str, GuardList] = {}
    # Run the samples one by one.
    for tq in _sample_run_info(spec.General, spec.Sample):
        # Do the transform and get status
        request_id = await adaptor.submit_transform(tq)
        status = await adaptor.get_transform_status(request_id)

        # Build the list of results.
        minio_results = MinioLocalAdaptor.for_transform(status)
        files = [
            await minio_results.get_signed_url(n.filename)
            for n in await minio_results.list_bucket()
        ]
        outputs = GuardList(files)

        title = tq.title if tq.title is not None else "local-run-dataset"
        results[title] = outputs

    return results


deliver = make_sync(deliver_async)
