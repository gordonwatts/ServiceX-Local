import pytest
from servicex_local import DockerCodegen
from pathlib import Path

from servicex_local import LocalXAODCodegen


def test_docker_codegen_xaod(tmp_path, request):
    "Do a basic func_adl uproot code generation from an official docker image"
    if not request.config.getoption("--docker"):
        pytest.skip("Use the --wsl2 pytest flag to run this test")

    # Create a dummy output json file for capabilities.
    output_location = tmp_path / "codegen_output"
    output_location.mkdir(parents=True, exist_ok=True)
    transform_file = tmp_path / "my_capabilities.json"
    transform_file.write_text("{}")

    # Run the code generator.
    codegen = DockerCodegen("sslhep/servicex_code_gen_raw_uproot:v1.5.4")
    query = '[{"treename": {"nominal": "modified"}, "filter_name": ["lbn"]}]'
    r = codegen.gen_code(query, output_location, transform_file)

    # Check the output makes sense!
    assert r == output_location

    # Check there is exactly one Python file in the directory
    py_files = list(Path(r).glob("*.py"))
    assert len(py_files) == 2, f"Expected 2 Python file, found {len(py_files)}"

    # Check there is only one file in the directory
    all_files = list(Path(r).iterdir())
    assert len(all_files) == 3, f"Expected 3 files, found {len(all_files)}"
    assert "transformer_capabilities.json" in [a.name for a in all_files]


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
    assert "runner.sh" in [a.name for a in all_files], "Expected runner.sh in directory"

    # Make sure the transformation capabilities file is there.
    assert (Path(r) / "transformer_capabilities.json").exists()

    assert len(all_files) == 7, f"Expected 7 file, found {len(all_files)}"
