import pytest
from servicex_local import DockerCodegen
from pathlib import Path

from servicex_local import LocalXAODCodegen


@pytest.mark.skip("This test requires docker to be installed")
def test_docker_codegen_xaod(tmp_path):
    "Do a basic func_adl uproot code generation from an official docker image"

    # Run the code generator.
    codegen = DockerCodegen("sslhep/servicex_code_gen_raw_uproot:v1.5.4")
    query = '[{"treename": {"nominal": "modified"}, "filter_name": ["lbn"]}]'
    r = codegen.gen_code(query, tmp_path)

    # Check the output makes sense!
    assert r == tmp_path

    # Check there is exactly one Python file in the directory
    py_files = list(Path(r).glob("*.py"))
    assert len(py_files) == 2, f"Expected 1 Python file, found {len(py_files)}"

    # Check there is only one file in the directory
    all_files = list(Path(r).iterdir())
    assert len(all_files) == 3, f"Expected 1 file, found {len(all_files)}"


def test_local_func_xAOD(tmp_path):

    query = (
        "(call Select (call Select (call EventDataset) (lambda (list e) "
        "(call (attr e 'Jets') 'AnalysisJets'))) (lambda (list jets) (dict "
        " (list 'pt' 'eta') (list (call (attr jets 'Select') (lambda (list j) "
        "(call (attr j 'pt')))) (call (attr jets 'Select') (lambda "
        "(list j) (call (attr j 'eta'))))))))"
    )

    codegen = LocalXAODCodegen()
    r = codegen.gen_code(query, tmp_path)
    all_files = list(Path(r).iterdir())
    assert len(all_files) == 6, f"Expected 1 file, found {len(all_files)}"
