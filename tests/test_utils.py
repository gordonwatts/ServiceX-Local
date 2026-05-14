import pytest
import uproot
import json
import awkward as ak
import numpy as np
import os
import re
import filecmp
from servicex_local import utils
from servicex import dataset
from servicex.python_dataset import PythonFunction
from servicex.dataset_identifier import (
    RucioDatasetIdentifier,
    FileListDataset,
    CERNOpenDataDatasetIdentifier,
    XRootDDatasetIdentifier,
)
from pathlib import Path


@pytest.fixture
def build_encoding_samples(tmp_path):

    test_path = str(tmp_path / "test_file1.root")
    # example data for two branches
    tree_data1 = {"branch1": np.ones(100), "branch2": np.zeros(100)}
    tree_data2 = {
        "branch1": np.ones(10),
    }

    # Create tmp .root files
    with uproot.create(test_path) as file:
        file.mktree("background", {"branch1": "float64", "branch2": "float64"})
        file["background"].extend(tree_data1)
        file.mktree("signal", {"branch1": "float64"})
        file["signal"].extend(tree_data2)

    return test_path


# Test run_query and print_structure_from_str
def test_encoding(build_encoding_samples, tmp_path, capsys):

    path = build_encoding_samples
    query_output = utils.run_query(path)

    # Check return types
    assert isinstance(
        query_output, ak.Array
    ), "run_query() does not produce an awkward.Array"
    encoded_str = query_output[0]
    assert isinstance(encoded_str, str), "run_query array content is not str"

    encoded_result = json.loads(encoded_str)

    # Check result
    expected_result = {
        "background": {"branch1": "AsDtype('>f8')", "branch2": "AsDtype('>f8')"},
        "signal": {"branch1": "AsDtype('>f8')"},
    }

    assert encoded_result == expected_result

    # Produce servicex.deliver() like dict
    # i.e {"Sample Name":"Path"}
    tree_data = {"branch": query_output}
    with uproot.create(tmp_path / "encoded.root") as file:
        file["servicex"] = tree_data
    assert os.path.exists(
        tmp_path / "encoded.root"
    ), "servicex-like test file not found."
    deliver_dict = {"test_file": [str(tmp_path / "encoded.root")]}

    # Test str formating on the deliver-like dict
    # save_to_txt
    utils.print_structure_from_str(deliver_dict, save_to_txt=True)
    out_txt = "samples_structure.txt"
    assert os.path.exists(out_txt), f"save_to_txt arg not producing {out_txt}"

    with open(out_txt, "r", encoding="utf-8") as f:
        written_str = f.read()

    # direct return
    output_str = utils.print_structure_from_str(deliver_dict)

    # do_print
    utils.print_structure_from_str(deliver_dict, do_print=True)
    captured = capsys.readouterr()

    # Check if all returns match
    assert (
        captured.out[0:-1] == written_str == output_str
    ), "saved, printed and direct return formats should not be different"

    # Compare with expected return
    test_txt = "tests/data/expected_structure.txt"
    assert filecmp.cmp(
        out_txt, test_txt
    ), "Formatted str does not match expected return"

    # Test filter_branch
    filtered_str = utils.print_structure_from_str(
        deliver_dict, filter_branch="branch2"
    )
    assert (
        "branch1" not in filtered_str
    ), "filter_branch argument is not removing branch1"


# Test spec builder for deliver
def test_spec_builder():
    # Get spec
    test_did_str = "random_space:did"
    spec = utils.build_deliver_spec(test_did_str)

    # Check return type
    assert isinstance(spec, dict), "build_deliver_spec does not return a dict"
    assert "Sample" in spec, "Key 'Sample' is missing in the returned dict"
    assert isinstance(spec["Sample"], list), "'Sample' should be a list"

    # Get return size
    size = len(spec["Sample"])
    assert size == 1, f"Only one did given but sample item of spec is not len 1: {size}"

    # Check first sample
    first_entry = spec["Sample"][0]
    assert isinstance(first_entry, dict), "Each entry in 'Sample' should be a dict"

    # Check each key type
    assert isinstance(first_entry["NFiles"], int), "'NFiles' should be an integer"
    assert isinstance(first_entry["Name"], str), "'Name' should be a string"

    assert isinstance(
        first_entry["Dataset"], RucioDatasetIdentifier
    ), "'Dataset' should be a RucioDatasetIdentifier"

    assert isinstance(
        first_entry["Query"], PythonFunction
    ), "'Query' should be a PythonFunction"

    # Different input types
    # list with two DIDs
    test_did_list = [test_did_str, test_did_str + "2"]
    spec_from_list = utils.build_deliver_spec(test_did_list)
    assert (
        len(spec_from_list["Sample"]) == 2
    ), "Wrong number of samples in deliver configuration"

    # dict with sample name
    test_did_dict = {"Custom-Name": test_did_str}
    spec_from_dict = utils.build_deliver_spec(test_did_dict)
    assert spec_from_dict["Sample"][0]["Name"] == "Custom-Name"

    # wrong input type
    wrong_did = 1234
    expected_msg = (
        f"Unsupported dataset input type: {type(wrong_did)}.\n"
        "Input must be str or list of str of Rucio DIDs, "
        "a DataSetIdentifier object or a dict "
        "('sample_name':'dataset_id')"
    )

    with pytest.raises(
        ValueError,
        match=re.escape(expected_msg),
    ):
        utils.build_deliver_spec(wrong_did)


def test_spec_builder_with_dataset_identifier():
    # Build multiple types of dataset identifiers
    ds1 = dataset.Rucio("random_space:did")
    ds2 = dataset.XRootD("root://server/file.root")
    ds3 = dataset.CERNOpenData("cernopendata:12345")
    ds4 = dataset.FileList(["file1.root", "file2.root"])

    ds_list = [ds1, ds2, ds3, ds4]
    ds_types = [
        RucioDatasetIdentifier,
        XRootDDatasetIdentifier,
        CERNOpenDataDatasetIdentifier,
        FileListDataset,
    ]
    for did, did_type in zip(ds_list, ds_types):
        spec = utils.build_deliver_spec(did)

        # Check return type
        assert isinstance(spec, dict), "build_deliver_spec does not return a dict"
        assert "Sample" in spec, "Key 'Sample' is missing in the returned dict"
        assert isinstance(spec["Sample"], list), "'Sample' should be a list"

        # Get return size
        size = len(spec["Sample"])
        assert (
            size == 1
        ), f"Only one did given but sample item of spec is not len 1: {size}"

        # Check first sample
        first_entry = spec["Sample"][0]
        assert isinstance(first_entry, dict), "Each entry in 'Sample' should be a dict"

        # Check each key type
        assert isinstance(first_entry["NFiles"], int), "'NFiles' should be an integer"
        assert isinstance(first_entry["Name"], str), "'Name' should be a string"

        assert isinstance(
            first_entry["Query"], PythonFunction
        ), "'Query' should be a PythonFunction"

        assert isinstance(
            first_entry["Dataset"], did_type
        ), (
            f"Input Dataset identifier {did} should be a {did_type} "
            f"but is {type(first_entry['Dataset'])}"
        )


def test_decoding_to_array(build_encoding_samples, array_out=True):
    path = build_encoding_samples
    query_output = utils.run_query(path)
    encoded_result = query_output[0]

    result = utils.str_to_array(encoded_result)

    # Test type
    assert isinstance(
        result, ak.types.arraytype.ArrayType
    ), "str_to_array does not return an awkward array type"
    expected_type_str = (
        "1 * {background: {branch1: var * float64, branch2: var * float64},"
        " signal: {branch1: var * float64}}"
    )
    assert str(result) == expected_type_str


@pytest.fixture
def build_test_samples(tmp_path):

    test_path = str(tmp_path / "test_metadata.root")

    # Create tmp .root files
    with uproot.create(test_path) as file:
        file.mktree("MetaData", {
            "FileMetaDataAuxDyn.test_100": "int64",
            "FileMetaDataAuxDyn.test_abc": str,
        })
        file["MetaData"].extend({
            "FileMetaDataAuxDyn.test_100": np.array([100], dtype="int64"),
            "FileMetaDataAuxDyn.test_abc": ["abc"],
        })

    return test_path


# Test run_query and print_structure_from_str
def test_metadata_retrieval(build_test_samples, tmp_path, capsys):

    path = build_test_samples
    query_output = utils.run_query(path)
    # Check result
    expected_result = {
        "FileMetaData": {"test_100": "100", "test_abc": "abc"},
        "MetaData": {
            "FileMetaDataAuxDyn.test_100": "AsDtype('>i8')",
            "FileMetaDataAuxDyn.test_abc": "AsStrings()",
        },
    }
    encoded_result = json.loads(query_output[0])

    assert encoded_result == expected_result

    # Produce servicex.deliver() like dict
    # i.e {"Sample Name":"Path"}
    tree_data = {"branch": query_output}
    with uproot.create(tmp_path / "encoded.root") as file:
        file["servicex"] = tree_data
    assert os.path.exists(
        tmp_path / "encoded.root"
    ), "servicex-like test file not found."
    deliver_dict = {"test_file": [str(tmp_path / "encoded.root")]}

    # Test str formating
    output_str = utils.print_structure_from_str(deliver_dict)

    expected_path = Path("tests/data/expected_metadata.txt")
    expected = expected_path.read_text(encoding="utf-8")

    assert (
        expected == output_str
    ), f"Output does not match expected.\n Output: {output_str}"
