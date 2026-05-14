LocalX Documentation
====================

This documentation describes LocalX, a library that simulates ServiceX behaviour on a local machine. It covers installation, configuration, and the workflow for running ServiceX-style requests against local files.

## What is LocalX

LocalX runs ServiceX workflows against local files, enabling testing and development without interfacing directly with the ServiceX backend. The interface mirrors ServiceX as closely as possible so that transitioning between the two requires minimal changes.

LocalX **is not** a replacement for ServiceX. It does not provide a self-hosted ServiceX instance; it only simulates the behaviour for testing and development. Access is limited to locally stored datasets — centrally stored datasets are not supported.

## When to use LocalX

LocalX is well-suited for the following use cases:

- Iterating on a query against a small local sample before scaling up to ServiceX
- Validating transformer behaviour in environments where the ServiceX backend is unavailable
- Developing and testing analysis code without consuming shared backend resources

## Other Resources

:::{admonition} ServiceX User Guide
:class: seealso
The [ServiceX User Guide](https://tryservicex.org/guide/) covers the core ServiceX concepts (Spec, Sample, datasets, queries) that LocalX mirrors.
:::

## Guide Format

This guide is organized into the following sections:

| Section | Description |
|---|---|
| [Install and Setup](setup.md) | How to install LocalX and pick a simulation platform |
| [Configuration](configuration.md) | How to configure LocalX with the `Config` and `xAODConfig` classes |
| [Using local_deliver](local_deliver.md) | How to submit a ServiceX Spec to the local backend |
| [Inspecting Local File Structure](local_get_structure.md) | How to explore ROOT file structure with `local_get_structure()` |

```{toctree}
:hidden:

setup
configuration
local_deliver
local_get_structure
```
