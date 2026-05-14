# Configuration

:::{admonition} You Will Learn:
:class: note
- What the LocalX `Config` class is and which settings it exposes
- How `xAODConfig` extends `Config` with backend-specific options
- Which quality-of-life helpers `xAODConfig` provides for analysis development
:::

This page describes how LocalX is configured. LocalX uses a `Config` class to describe the simulated backend; specific backends extend it with their own settings. The resulting `Config` object is later passed to `local_deliver()` — see [Using local_deliver](local_deliver.md).

:::{note}
LocalX currently supports the xAOD backend only. Additional backends will be added as their transformers become available.
:::

## Base Config Class

All `Config` subclasses inherit the following fields and default values:

```python
version: str = "latest"
platform: Union[Platform, str] = "docker"
ignore_cache: bool = False
awk: bool = False
logging_level: str = "WARNING"
```

### Platforms

LocalX supports three platforms for simulating the ServiceX backend:

- `docker`
- `singularity`
- `wsl2`

The default platform is `docker`, which must be installed locally. For platform installation details, see [Install and Setup](setup.md).

### awk Setting

Many xAOD workflows import the resulting data into Awkward Array using the `to_awk` function from the `servicex_analysis_utils` package. Setting `awk=True` causes LocalX to perform this conversion automatically.

:::{note}
The `awk` setting is a LocalX convenience for quick testing. ServiceX itself does not perform this conversion server-side, so workflows that rely on `awk=True` must run `to_awk` explicitly when moved to ServiceX.
:::

## Using xAOD

The xAOD backend is configured with the `xAODConfig` class:

```python
from servicex_local import xAODConfig

xAOD_config = xAODConfig(
    release=25,
)
```

The `release` option is unique to `xAODConfig` and selects the ATLAS release to use. When `version` is left at its default of `"latest"`, LocalX selects the most recent supported version for the chosen release.

### xAOD Config Quality-of-Life Features

`xAODConfig` provides two helpers for analysis development.

**Listing available release versions.** The full list of release versions available in the configured docker container is accessed via the `available_versions` property:

```python
xAOD_config.available_versions
```

**Generating the query object from the config.** Analysis development often requires changing release versions quickly. The query object can be built directly from the config, avoiding the need to import `func_adl_servicex_xaodrXX` and update it in two places:

```python
query = xAOD_config.FuncADLQueryPHYS()
```
