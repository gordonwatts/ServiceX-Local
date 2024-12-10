import pytest
from servicex_local import WSL2ScienceImage, LocalXAODCodegen, SXLocalAdaptor
from servicex import query as q, deliver, dataset
import logging


@pytest.mark.skip(reason="This integration test is not ready to run")
def test_adaptor_xaod_wsl2():
    codegen = LocalXAODCodegen()
    science_runner = WSL2ScienceImage("atlas_al9", "22.2.107")
    adaptor = SXLocalAdaptor(codegen, science_runner)

    logging.basicConfig(level=logging.DEBUG)

    # The simple query, take straight from the example in the documentation.
    query = q.FuncADL_ATLASr22()  # type: ignore
    jets_per_event = query.Select(lambda e: e.Jets("AnalysisJets"))
    jet_info_per_event = jets_per_event.Select(
        lambda jets: {
            "pt": jets.Select(lambda j: j.pt()),
            "eta": jets.Select(lambda j: j.eta()),
        }
    )

    spec = {
        "Sample": [
            {
                "Name": "func_adl_xAOD_simple",
                "Dataset": dataset.FileList(
                    [
                        "tests/test.root",  # noqa: E501
                    ]
                ),
                "Query": jet_info_per_event,
            }
        ]
    }
    files = deliver(spec, servicex_name="servicex-uc-af")  # , sx_adaptor=adaptor)
    assert files is not None, "No files returned from deliver! Internal error"
    assert False
