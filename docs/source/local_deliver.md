# Using local_deliver

:::{admonition} You Will Learn:
:class: note
- How `local_deliver()` relates to ServiceX's `deliver()`
- Why `local_deliver()` requires a LocalX `Config` object
- How to assemble a ServiceX Spec and submit it to the local backend
:::

This page describes `local_deliver()`, the LocalX entry point that transforms a ServiceX Spec against local files.

## Relationship to deliver()

`local_deliver()` mirrors ServiceX's `deliver()` function so that workflows can be moved between LocalX and ServiceX with minimal changes. Like `deliver()`, it accepts a ServiceX Spec consisting of one or more Samples, each containing a dataset and a query. Unlike `deliver()`, it also requires a LocalX `Config` object that describes the simulated backend.

:::{admonition} ServiceX deliver()
:class: seealso
For background on the ServiceX Spec, Samples, and the structure of a request, see [Understanding deliver()](https://tryservicex.org/guide/deliver.html) in the ServiceX User Guide.
:::

## Prerequisites

Before calling `local_deliver()`, the following are required:

- A LocalX `Config` object describing the simulated backend — see [Configuration](configuration.md).
- A query built for the chosen backend (for the xAOD backend, this is typically a `FuncADLQueryPHYS` object).
- One or more local files referenced through a ServiceX dataset such as `dataset.FileList`.

## Submitting a Spec

The example below builds a Spec with a single Sample and submits it through `local_deliver()` using an xAOD configuration:

```python
from servicex import dataset, ServiceXSpec, Sample
from servicex_local import xAODConfig, local_deliver

xAOD_config = xAODConfig(
    release=25,
    platform="docker",
    awk=True,
)

spec = ServiceXSpec(
    Sample=[
        Sample(
            Name="Sample1",
            Dataset=dataset.FileList([dataset_path]),
            Query=query,
        ),
    ]
)

data = local_deliver(spec, xAOD_config)
```

When the `awk` setting on the `Config` is `False`, `local_deliver()` returns the same dictionary as ServiceX's `deliver()`: sample name to list of file paths.

When `awk=True`, the result is passed through `to_awk` from `servicex_analysis_utils`. For a Spec with a single Sample, `local_deliver()` returns that sample's Awkward Array directly. For a Spec with multiple Samples, it returns a dictionary mapping sample name to Awkward Array.
