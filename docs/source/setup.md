# Install and Setup

:::{admonition} You Will Learn:
:class: note
- How to install LocalX from PyPI
- Which platforms LocalX supports for simulating the ServiceX backend
- Where to find platform-specific setup instructions
:::

This page covers installing the LocalX client and selecting a platform to simulate the ServiceX backend.

## Installation

LocalX is installed from PyPI with `pip`:

```bash
pip install servicex-local
```

## Choosing a Platform

LocalX simulates the ServiceX backend through a containerized runtime. One of the following platforms is required:

| Platform | Notes |
|---|---|
| Docker | Default. Recommended for local development on Linux, macOS, and Windows. |
| Singularity / Apptainer | Used on shared HPC systems where Docker is unavailable. |
| WSL2 | Used on Windows environments without native Docker support. |

Users running on an analysis facility should use whichever platform the facility provides.

Refer to the platform-specific installation documentation for setup details. The selected platform is later supplied to LocalX through the [`Config`](configuration.md) object.
