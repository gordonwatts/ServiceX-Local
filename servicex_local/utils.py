import logging
from .configurations import Config
from servicex import query, dataset
import uproot
import numpy as np
import awkward as ak
import json
from servicex.dataset_identifier import DataSetIdentifier


def run_query(
    input_filenames,
):
    import uproot
    import awkward as ak
    import json

    def is_tree(obj):
        """Helper to check if a root file item is TTree."""
        if hasattr(obj, "classname"):
            cls_attr = obj.classname
            cls_value = cls_attr() if callable(cls_attr) else cls_attr
            return "TTree" in cls_value
        elif hasattr(obj, "classnames"):
            cls_attr = obj.classnames
            cls_values = cls_attr() if callable(cls_attr) else cls_attr
            return any("TTree" in cls for cls in cls_values)
        return False

    """
    Opens a ROOT file and returns a JSON-formatted string describing the structure,
    encoded inside an ak.Array for ServiceX.
    """
    tree_dict = {}

    with uproot.open(input_filenames) as file:

        for tree_name in file.keys():
            tree_name_clean = tree_name.rstrip(";1")
            tree = file[tree_name]

            if not is_tree(tree):
                continue

            if tree_name_clean == "MetaData":
                fm_branches = [
                    b for b in tree.keys() if b.startswith("FileMetaDataAuxDyn.")
                ]
                # remove the prefix in keys
                meta_dict = {
                    p[19:]: str(tree[p].array(library="ak")[0]) for p in fm_branches
                }
                tree_dict["FileMetaData"] = meta_dict

            branch_dict = {}
            for branch_name, branch in tree.items():
                branch_type = str(branch.interpretation)
                branch_dict[branch_name] = branch_type

            tree_dict[tree_name_clean] = branch_dict

    # Serialize tree_dict to JSON string
    json_str = json.dumps(tree_dict)

    # Return JSON string wrapped in an awkward array
    return ak.Array([json_str])


def build_deliver_spec(datasets):
    """
    Helper to build the servicex.deliver configuration.
    Supports multiple inputs for multiple sample queries.

    Parameters:
    datasets (str, [str], dict, DataSetIdentifier): Rucio DIDs (str) or DataSetIdentifier.
                                                    If dict, custom names can be given per dataset

    Returns:
    spec_python (dict): The specification for the python function query containing
                        Name, Query, Dataset, NFiles
    """
    # Servicex query using the PythonFunction backend
    query_PythonFunction = query.PythonFunction().with_uproot_function(run_query)

    # Create a dict with sample name for ServiceX query & datasetID
    dataset_dict = {}
    user_in = type(datasets)

    # TODO: Add processing of the URL to make sure the it exists

    user_in = type(datasets)

    # Rucio DID as str
    if user_in == str:
        dataset_dict.update({datasets: datasets})  # Use dataset ID as sample name
    elif user_in == list and type(datasets[0]) is str:
        for ds in datasets:
            dataset_dict.update({ds: ds})
    elif user_in == dict:  # Custom sample names
        dataset_dict = datasets
    # Single DataSetIdentifier object
    elif isinstance(datasets, DataSetIdentifier):
        dataset_dict.update({"Dataset": datasets})
    else:
        raise ValueError(
            f"Unsupported dataset input type: {user_in}.\n"
            "Input must be str or list of str of Rucio DIDs, "
            "a DataSetIdentifier object or a dict ('sample_name':'dataset_id')"
        )

    sample_list = [
        {
            "NFiles": 1,
            "Name": name,
            "Dataset": did if isinstance(did, DataSetIdentifier) else dataset.Rucio(did),
            "Query": query_PythonFunction,
        }
        for name, did in dataset_dict.items()
    ]
    spec_python = {"Sample": sample_list}

    return spec_python


def open_delivered_file(sample, path):
    """
    Opens the first file delivered by ServiceX for a given sample and returns the
    structure encoded in the "servicex/branch" branch.
    If no files are found, logs a warning and returns None.
    Parameters:
      sample (str): The sample name for which to open the file.
      path (list): List of file paths delivered by ServiceX for the sample.
    """

    if not path:
        logging.warning(
            f"Warning: No files found for sample '{sample}' in delivered results. Skipping."
        )
        return None

    try:
        with uproot.open(path[0]) as f:
            return f["servicex"]["branch"].array()[0]
    except Exception as e:
        logging.error(f"Error opening file for sample '{sample}': {e}")
        return None


def print_structure_from_str(
    json_by_sample, filter_branch="", save_to_txt=False, do_print=False
):
    """
    Re-formats the JSON structure string from ServiceX into a readable summary.

    Parameters:
      json_by_sample (dict): mapping of sample names to JSON structure strings.
      filter_branch (str): If provided, only branches containing this string are included.
      save_to_txt (bool): If True, saves output to a text file instead of returning it.
      do_print (bool): If True, prints the output to the terminal and returns None.

    Returns:
      result_str (str): The formatted file structure.
    """
    output_lines = []

    for sample_name, structure_str in json_by_sample.items():
        if structure_str is None:
            continue
        if isinstance(structure_str, list):
            structure_str = open_delivered_file(sample_name, structure_str)
        if structure_str is None:
            continue
        structure_dict = json.loads(structure_str)

        output_lines.append(
            "\n---------------------------\n"
            f"\U0001f4c1 Sample: {sample_name}\n"
            "---------------------------"
        )

        # Get the metadata first
        output_lines.append("\nFile Metadata \u2139\ufe0f :\n")
        if "FileMetaData" not in structure_dict:
            output_lines.append("No FileMetaData found in dataset.")
        else:
            for key, value in structure_dict.get("FileMetaData", {}).items():
                output_lines.append(f"── {key}: {value}")
        output_lines.append("\n---------------------------")

        # drop the File metadata from the trees
        structure_dict.pop("FileMetaData", {})

        output_lines.append(
            f"\nFile structure with branch filter \U0001f33f '{filter_branch}':\n"
        )

        for tree_name, branches in structure_dict.items():
            output_lines.append(f"\n\U0001f333 Tree: {tree_name}")
            output_lines.append("   ├── Branches:")
            for branch_name, dtype in branches.items():
                if filter_branch and filter_branch not in branch_name:
                    continue
                output_lines.append(f"   │   ├── {branch_name} ; dtype: {dtype}")

    result_str = "\n".join(output_lines).encode("utf-8").decode("utf-8")

    if save_to_txt:
        with open("samples_structure.txt", "w", encoding="utf-8") as f:
            f.write(result_str)
        return "File structure saved to 'samples_structure.txt'."
    elif do_print:
        print(result_str)
        return
    else:
        return result_str


def parse_jagged_depth_and_dtype(dtype_str):
    """
    Helper to decode the dtype str for each branch.

    Parses uproot-style interpretation strings such as:
    - "AsJagged(AsJagged(AsDtype('>f4')))"

    Returns the number of nested layers and the inner dtype.
    Used in str_to_array to reconstruct the ak.array.

    Parameters:
        dtype_str (str): The dtype part of a branch info str; from the delivered file structure.

    Returns:
        int, str: jagged_depth, base_numpy_dtype_str or None if not recognized.
    """
    depth = 0
    current = dtype_str.strip()

    # Count how many nested AsJagged(...) wrappers exist
    while current.startswith("AsJagged("):
        depth += 1
        current = current[
            len("AsJagged("):-1
        ].strip()  # Strip outermost wrapper, up to -1 to remove )

    # Extract the base dtype string from AsDtype('<np-format>')
    if current.startswith("AsDtype('") and current.endswith("')"):
        base_dtype = current[len("AsDtype('"):-2]
        return depth, base_dtype
    else:
        return depth, None


def str_to_array(encoded_json_str):
    """
    Helper to reconstruct ak.Arrays from a JSON-formatted file-structure string.
    Returns an array mimicking TTrees and TBranches with correct field names and dtypes.

    Parameters:
        encoded_json_str (str): JSON string from run_query.

    Returns:
        ak.Array: An array containing a dictionary of trees with branch structures
                  and dummy typed values.
    """
    reconstructed_data = {}
    structure_dict = json.loads(encoded_json_str)
    # drop the File metadata from the trees
    structure_dict.pop("FileMetaData", {})

    for treename, branch_dict in structure_dict.items():
        branches = {}

        for branch_name, dtype_str in branch_dict.items():
            # Get jagged depth and numpy base dtype
            depth, base_dtype_str = parse_jagged_depth_and_dtype(dtype_str)
            if base_dtype_str is None:
                branches[branch_name] = None
                continue

            try:
                np_dtype = np.dtype(base_dtype_str)
            except TypeError:
                branches[branch_name] = None
                continue

            dummy = np_dtype.type(0)
            for _ in range(depth):
                dummy = [dummy]

            branches[branch_name] = ak.Array([dummy])

        if branches:
            reconstructed_data[treename] = ak.Array([branches])

    return ak.Array(reconstructed_data).type


def local_get_structure(datasets, config: Config, array_out=False, **kwargs):
    """
    Utility function.
    Reads the structure of local ROOT files directly using uproot.
    Calls print_structure_from_str() to dump the structure in a user-friendly format.

    Parameters:
      datasets (dict,str,[str]): The datasets from which to print the file structures.
                                A custom sample name per dataset can be given in a dict form:
                                {'sample_name':'file_path'}
      kwargs : Arguments to be propagated to print_structure_from_str
    """
    user_in = type(datasets)
    dataset_dict = {}
    if user_in == str:
        dataset_dict[datasets] = datasets
    elif user_in == list and type(datasets[0]) is str:
        for ds in datasets:
            dataset_dict[ds] = ds
    elif user_in == dict:
        dataset_dict = datasets
    elif isinstance(datasets, DataSetIdentifier):
        dataset_dict["Dataset"] = datasets
    else:
        raise ValueError(
            f"Unsupported dataset input type: {user_in}.\n"
            "Input must be str or list of str of file paths, "
            "a DataSetIdentifier object or a dict ('sample_name':'file_path')"
        )

    json_by_sample = {}
    for sample_name, file_path in dataset_dict.items():
        result = run_query(file_path)
        json_by_sample[sample_name] = str(result[0])

    if array_out:
        return {name: str_to_array(s) for name, s in json_by_sample.items()}
    return print_structure_from_str(json_by_sample, **kwargs)
