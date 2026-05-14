# Inspecting Local File Structure

:::{admonition} You Will Learn:
:class: note
- How to inspect the branch structure of local ROOT files with `local_get_structure()`
- How to filter branches, print output, and save results to a text file
- How to get an Awkward Array type representation of the file structure
:::

This page describes `local_get_structure()`, a utility that reads the TTree and branch structure of local ROOT files directly — without going through ServiceX or a container backend.

## Overview

`local_get_structure()` is useful when you want to quickly explore a ROOT file before writing a query. It reads the file structure using `uproot` and formats the output as a readable tree summary showing each TTree, its branches, and their dtypes.

## Basic Usage

Pass one or more file paths and a LocalX `Config` object:

```python
from servicex_local import local_get_structure, xAODConfig

config = xAODConfig(release=25)

result = local_get_structure("path/to/file.root", config)
print(result)
```

The return value is a formatted string. Example output:

```
---------------------------
📁 Sample: path/to/file.root
---------------------------

File Metadata ℹ️ :

No FileMetaData found in dataset.

---------------------------

File structure with branch filter 🌿 '':


🌳 Tree: background
   ├── Branches:
   │   ├── branch1 ; dtype: AsDtype('>f8')
   │   ├── branch2 ; dtype: AsDtype('>f8')

🌳 Tree: signal
   ├── Branches:
   │   ├── branch1 ; dtype: AsDtype('>f8')
```

## Dataset Input Formats

`local_get_structure()` accepts the same input formats as `local_deliver()`:

| Input type | Behaviour |
|---|---|
| `str` | Single file path. The path is used as the sample name. |
| `list[str]` | Multiple file paths, each used as its own sample name. |
| `dict` | Maps custom sample names to file paths: `{"my_sample": "path/to/file.root"}`. |

```python
# Multiple files
result = local_get_structure(["file1.root", "file2.root"], config)

# Custom sample names
result = local_get_structure({"signal": "sig.root", "background": "bkg.root"}, config)
```

## Filtering Branches

Use the `filter_branch` keyword argument to show only branches whose names contain a given string:

```python
result = local_get_structure("file.root", config, filter_branch="Electron")
```

Only branches with `"Electron"` in their name will appear in the output.

## Printing Directly

Pass `do_print=True` to print the structure to the terminal instead of returning a string:

```python
local_get_structure("file.root", config, do_print=True)
```

When `do_print=True`, the function returns `None`.

## Saving to a File

Pass `save_to_txt=True` to write the output to `samples_structure.txt` in the current directory:

```python
local_get_structure("file.root", config, save_to_txt=True)
```

The function returns the message `"File structure saved to 'samples_structure.txt'."` when this option is used.

## Getting an Array Type Representation

Pass `array_out=True` to get an Awkward Array type object instead of the formatted string. This returns a dictionary mapping each sample name to an `ak.Array` type that mirrors the TTree structure with correct field names and dtypes:

```python
types = local_get_structure("file.root", config, array_out=True)
```

This is useful for verifying that branch names and types match what a query expects before running through ServiceX.

:::{note}
`array_out=True` and `save_to_txt=True` / `do_print=True` are mutually exclusive. When `array_out=True` is set, the formatting keyword arguments are ignored.
:::
