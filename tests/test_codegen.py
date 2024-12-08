from servicex_local import DockerCodegen
from pathlib import Path


def test_docker_codegen_xaod(tmp_path):
    "Do a basic func_adl uproot code generation from an official docker image"

    # Run the code generator.
    codegen = DockerCodegen("sslhep/servicex_code_gen_func_adl_uproot:v1.5.4")
    query = "(Select (call EventDataset) (lambda (list event) (dict (list 'pt' 'eta') "
    "(list (attr event 'Muon_pt') (attr event 'Muon_eta')))))"
    r = codegen.gen_code(query, tmp_path)

    # Check the output makes sense!
    assert r == tmp_path

    # Check there is exactly one Python file in the directory
    py_files = list(Path(r).glob("*.py"))
    assert len(py_files) == 1, f"Expected 1 Python file, found {len(py_files)}"

    # Check there is only one file in the directory
    all_files = list(Path(r).iterdir())
    assert len(all_files) == 1, f"Expected 1 file, found {len(all_files)}"
