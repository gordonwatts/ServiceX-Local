import pytest
from servicex_local.codegen import DockerCodegen
from servicex_local.science_images import DockerScienceImage


@pytest.mark.skip(reason="This test needs docker to be installed")
def test_katan_example(tmp_path):
    "Run simple"

    codegen = DockerCodegen("sslhep/servicex_code_gen_raw_uproot:v1.5.4")
    docker = DockerScienceImage("sslhep/servicex_func_adl_uproot_transformer:uproot5")

    input_files = [
        "root://fax.mwt2.org:1094//pnfs/uchicago.edu/atlaslocalgroupdisk/"
        "rucio/user/mgeyik/e7/ee/user.mgeyik.30182995._000093.out.root"
    ]
    query = '[{"treename": {"nominal": "modified"}, "filter_name": ["lbn"]}]'

    # Run the code generator.
    code_dir = tmp_path / "code"
    code_dir.mkdir()
    r = codegen.gen_code(query, tmp_path / "code")

    # Next, run on the generated code
    result_dir = tmp_path / "result"
    result_dir.mkdir()
    output_files = docker.transform(r, input_files, result_dir, "root-file")

    assert len(output_files) == 1
