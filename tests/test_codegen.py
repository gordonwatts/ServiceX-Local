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
    assert "runner.sh" in [a.name for a in all_files], "Expected runner.sh in directory"


def test_local_has_right_line_endings(tmp_path):
    "Make sure all .sh files generated have Linux line-endings"

    query = (
        "(call Select (call Select (call EventDataset) (lambda (list e) "
        "(call (attr e 'Jets') 'AnalysisJets'))) (lambda (list jets) (dict "
        " (list 'pt' 'eta') (list (call (attr jets 'Select') (lambda (list j) "
        "(call (attr j 'pt')))) (call (attr jets 'Select') (lambda "
        "(list j) (call (attr j 'eta'))))))))"
    )

    codegen = LocalXAODCodegen()
    r = codegen.gen_code(query, tmp_path)
    all_files = list(Path(r).glob("*.sh"))
    for f in all_files:
        with open(f, "rb") as file:
            content = file.read()
            assert b"\r\n" not in content, f"File {f} has Windows line endings"
            assert b"\r" not in content, f"File {f} has Mac line endings"
            assert b"\n" in content, f"File {f} has no line endings"
